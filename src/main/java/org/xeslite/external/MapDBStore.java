package org.xeslite.external;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.prefs.Preferences;

import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.extension.std.XLifecycleExtension;
import org.deckfour.xes.extension.std.XTimeExtension;
import org.deckfour.xes.model.XAttributable;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XLog;
import org.mapdb.Atomic;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBException;
import org.mapdb.Fun;
import org.mapdb.Fun.Pair;
import org.mapdb.Serializer;
import org.xeslite.common.XESLiteException;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;

/**
 * Implementation using the BTree implementation of MapDB
 * (https://github.com/jankotek/mapdb) as back-end.
 * 
 * Attributes are stored in a {@link BTreeMap} with a single {@link Long} as
 * key.
 * 
 * @author F. Mannhardt
 * 
 */
public final class MapDBStore extends ExternalStoreAbstract {

	public static final Preferences PREFS = Preferences.userRoot().node("org.processmining.xeslite.external");

	private static final int DEFAULT_KEY_POOL_SHIFT = 12; // 4096  should be enough for most XES logs
	private static final int MAX_KEY_POOL_SHIFT = 20; // 1048576 unique attribute, this would be a strange XES file
	private static final int DEFAULT_NODE_SIZE = 32; // 32 is a good compromise for concurrent writes

	public static int getDefaultKeyPoolShift() {
		return PREFS.getInt("defaultKeyPoolShift", DEFAULT_KEY_POOL_SHIFT);
	}

	public static int getMaxKeyPoolShift() {
		return PREFS.getInt("maxKeyPoolShift", MAX_KEY_POOL_SHIFT);
	}

	public static int getDefaultNodeSize() {
		return PREFS.getInt("defaultNodeSize", DEFAULT_NODE_SIZE);
	}

	private static final class ReversedLongSerializer extends BTreeKeySerializer<Long, long[]> {

		// Reverse

		public int compare(long[] keys, int pos1, int pos2) {
			return -BTreeKeySerializer.LONG.compare(keys, pos1, pos2);
		}

		public int compare(long[] keys, int pos, Long key) {
			return -BTreeKeySerializer.LONG.compare(keys, pos, key);
		}

		public Comparator<Long> comparator() {
			return Ordering.from(BTreeKeySerializer.LONG.comparator()).reverse();
		}

		// Forward method to original serializer

		public void serialize(DataOutput out, long[] keys) throws IOException {
			BTreeKeySerializer.LONG.serialize(out, keys);
		}

		public long[] deserialize(DataInput in, int nodeSize) throws IOException {
			return BTreeKeySerializer.LONG.deserialize(in, nodeSize);
		}

		public Long getKey(long[] keys, int pos) {
			return BTreeKeySerializer.LONG.getKey(keys, pos);
		}

		public long[] emptyKeys() {
			return BTreeKeySerializer.LONG.emptyKeys();
		}

		public int length(long[] keys) {
			return BTreeKeySerializer.LONG.length(keys);
		}

		public long[] putKey(long[] keys, int pos, Long newKey) {
			return BTreeKeySerializer.LONG.putKey(keys, pos, newKey);
		}

		public long[] copyOfRange(long[] keys, int from, int to) {
			return BTreeKeySerializer.LONG.copyOfRange(keys, from, to);
		}

		public long[] deleteKey(long[] keys, int pos) {
			return BTreeKeySerializer.LONG.deleteKey(keys, pos);
		}

		public long[] arrayToKeys(Object[] keys) {
			return BTreeKeySerializer.LONG.arrayToKeys(keys);
		}
	}

	final class PumpServiceImpl implements PumpService {

		private final class PumpIterator implements Iterator<Fun.Pair<Long, ExternalAttribute>> {

			private final class AttributeComparator implements Comparator<XAttribute> {
				public int compare(XAttribute a1, XAttribute a2) {
					int key1, key2 = 0;
					if (a1 instanceof ExternalAttribute && ((ExternalAttribute) a1).getStore() == MapDBStore.this) {
						key1 = ((ExternalAttribute) a1).getInternalKey();
					} else {
						assert a1.getKey() != null : "Key null for " + a1;
						key1 = keyPool.put(a1.getKey());
					}
					if (a2 instanceof ExternalAttribute && ((ExternalAttribute) a2).getStore() == MapDBStore.this) {
						key2 = ((ExternalAttribute) a2).getInternalKey();
					} else {
						assert a2.getKey() != null : "Key null for " + a2;
						key2 = keyPool.put(a2.getKey());
					}
					return Ints.compare(key1, key2);
				}
			}

			private ExternalAttributable owner;
			private Iterator<XAttribute> attributes = ImmutableSet.<XAttribute>of().iterator();
			private Comparator<? super XAttribute> comparator = new AttributeComparator();

			public Pair<Long, ExternalAttribute> next() {
				assert owner != null;
				assert owner.getExternalId() != Long.MIN_VALUE;
				ExternalAttribute a = XAttributeExternalImpl.convert(MapDBStore.this, owner, attributes.next());
				Long compositeKey = getCompositeKey(owner, a.getInternalKey());
				return new Pair<>(compositeKey, a);
			}

			public boolean hasNext() {
				try {
					if (attributes == null) {
						assert owner == null;
						return false;
					}
					if (attributes.hasNext()) {
						assert owner != null;
						return true;
					}
					Pair<XAttributable, List<XAttribute>> attributesWithOwner = takeNext();
					if (attributesWithOwner != endOfPump) {
						sort(attributesWithOwner.b);
						owner = (ExternalAttributable) attributesWithOwner.a;
						attributes = attributesWithOwner.b.iterator();
						if (!attributes.hasNext()) {
							throw new XESLiteException("Empty list of attributes is not allowed!");
						}
						return true;
					} else {
						return endOfData();
					}
				} catch (InterruptedException e) {
					return endOfData();
				}
			}

			private final Pair<XAttributable, List<XAttribute>> takeNext() throws InterruptedException {
				return pumpQueue.take();
			}

			private final boolean endOfData() {
				owner = null;
				attributes = null;
				return false;
			}

			private final void sort(List<XAttribute> attributes) {
				// Sort attributes in increasing order for pump
				Collections.sort(attributes, comparator);
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		}

		private final ExecutorService pumpExecutor = Executors.newSingleThreadExecutor();
		private final Future<ConcurrentNavigableMap<Long, ExternalAttribute>> pumpFuture;

		private final BlockingQueue<Pair<XAttributable, List<XAttribute>>> pumpQueue = new LinkedTransferQueue<>();
		private final Iterator<Pair<Long, ExternalAttribute>> pumpIterator;

		private final Pair<XAttributable, List<XAttribute>> endOfPump = new Pair<>(null, null);

		/**
		 * Starts the pump thread. Side-effect: The 'db' will be locked!
		 */
		public PumpServiceImpl(final Builder builder) {
			db.delete(".attributeStore");
			assert idFactory instanceof IdFactorySeq : "Keys need to be sorted in sequential ascending order for data pump. "
					+ "Please choose an appropriate IdFactory such as IdFactorySeq.";
			pumpIterator = new PumpIterator();
			final Thread callingThread = Thread.currentThread(); // Use to interrupt calling thread upon error on pump thread
			// Auto-start the creation thread - be aware that 'db' is locked now!
			// So it cannot be used until finish pump is called!
			pumpFuture = pumpExecutor.submit(new Callable<ConcurrentNavigableMap<Long, ExternalAttribute>>() {

				public ConcurrentNavigableMap<Long, ExternalAttribute> call() throws Exception {
					try {
						return db.treeMapCreate(".attributeStore")
								.keySerializer(new BTreeKeySerializer.Compress(new ReversedLongSerializer()))
								.valueSerializer(serializer).nodeSize(builder.nodeSize).pumpSource(pumpIterator)
								.makeOrGet();
					} catch (Exception e) {
						callingThread.interrupt();
						throw e;
					}
				}
			});

		}

		public void pumpAttributes(XAttributable attributable, List<XAttribute> attributes) {
			if (attributable instanceof ExternalAttributable) {
				if (!attributes.isEmpty()) {
					// Handle caching of attribute as we are by-passing the usual adding of attributes
					// This is done on the adding thread to avoid race conditions  
					if (attributable instanceof AttributesCacheable) {
						for (Iterator<XAttribute> iterator = attributes.iterator(); iterator.hasNext();) {
							XAttribute a = iterator.next();
							AttributesCacheable cacheable = (AttributesCacheable) attributable;
							Integer cacheIndex = cacheable.getCacheIndex(a.getKey());
							if (cacheIndex != null) {
								cacheable.setCacheValue(cacheIndex, a);
								iterator.remove();
							}
						}
					}
					if (!attributes.isEmpty()) {
						Pair<XAttributable, List<XAttribute>> pair = new Pair<>(attributable, attributes);
						try {
							pumpQueue.offer(pair, 30, TimeUnit.DAYS);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					} // else just ignore this one
				}
			} else {
				// just put the attribute
				for (XAttribute attr : attributes) {
					attributable.getAttributes().put(attr.getKey(), attr);
				}
			}
		}

		public void finishPump() {
			try {
				pumpQueue.offer(endOfPump, 30, TimeUnit.DAYS);
				try {
					attributeStorage = pumpFuture.get();
				} catch (ExecutionException e) {
					reThrow(e);
				}
			} catch (InterruptedException e) {
				// Catch to find out the real reason for this
				try {
					pumpFuture.get(1, TimeUnit.SECONDS);
					// There was no exception, just interrupt again
					Thread.currentThread().interrupt();
				} catch (ExecutionException | TimeoutException | InterruptedException e1) {
					reThrow(e1);
				}
			} finally {
				pumpExecutor.shutdown();
				MapDBStore.this.pumpService = null; // Remove reference to ourselves for GC
			}
		}

		private final void reThrow(Exception e) {
			if (e.getCause() instanceof RuntimeException) {
				throw (RuntimeException) e.getCause();
			}
			throw new XESLiteException(e);
		}

	}

	public static final class Builder {

		private MapDBDatabase database;
		private boolean isPump = false;

		private int keyPoolShift = getDefaultKeyPoolShift();
		private int nodeSize = getDefaultNodeSize();

		public Builder withDatabase(MapDBDatabase database) {
			this.database = database;
			return this;
		}

		public Builder withPump() {
			this.isPump = true;
			return this;
		}

		public Builder withKeyPoolShift(int keyPoolShift) {
			this.keyPoolShift = keyPoolShift;
			return this;
		}

		public Builder withNodeSize(int nodeSize) {
			this.nodeSize = nodeSize;
			return this;
		}

		public ExternalStore build() {
			if (database == null)
				// Stand-alone mode with temporary file
				database = new MapDBDatabaseImpl();
			try {
				database.createDB();
			} catch (IOException e) {
				throw new XESLiteException("Failed to create MapDB database!", e);
			} catch (DBException.FileLocked e) {
				throw new XESLiteException("Failed to create MapDB database! " + e.getMessage(), e);
			} catch (DBException e) {
				throw new XESLiteException("Failed to create MapDB database!", e);
			}
			return new MapDBStore(this);
		}

	}

	private final Builder builder;

	private PumpService pumpService;

	private final DB db;
	private final MapDBAttributeSerializer serializer;

	private NavigableMap<Long, ExternalAttribute> attributeStorage;

	private final IdFactory idFactory;
	private final StringPool keyPool;
	private final StringPool literalPool;

	private MapDBStore(Builder builder) {
		super();
		this.builder = builder;

		db = builder.database.getDB();

		if (builder.isPump) {
			// sequential ID's are required for data pump
			idFactory = new IdFactorySeq(builder.keyPoolShift);
			keyPool = createKeyPool(db, idFactory.getIntervalCapacity());
			literalPool = createLiteralPool(db);
			serializer = createSerializer(literalPool, keyPool);
		} else {
			idFactory = createIDFactory(db);
			keyPool = createKeyPool(db, idFactory.getIntervalCapacity());
			literalPool = createLiteralPool(db);
			serializer = createSerializer(literalPool, keyPool);
			if (db.exists(".attributeStore")) {
				attributeStorage = db.treeMap(".attributeStore", new ReversedLongSerializer(), serializer);
			} else {
				attributeStorage = db.treeMapCreate(".attributeStore")
						.keySerializer(new BTreeKeySerializer.Compress(new ReversedLongSerializer())) //
						.valueSerializer(serializer) //
						.nodeSize(builder.nodeSize) //
						.makeOrGet();
			}
		}

	}

	private MapDBAttributeSerializer createSerializer(StringPool literalPool, StringPool keyPool) {
		Integer nameIndex = keyPool.put(XConceptExtension.KEY_NAME);
		Integer transitionIndex = keyPool.put(XLifecycleExtension.KEY_TRANSITION);
		keyPool.put(XTimeExtension.KEY_TIMESTAMP); // is not pooled as it is not literal
		Set<Integer> keysToPool = ImmutableSet.of(nameIndex, transitionIndex);
		MapDBAttributeSerializer serializer = new MapDBAttributeSerializer(literalPool, keysToPool);
		return serializer;
	}

	private static StringPool createKeyPool(DB db, int fixedCapacity) {
		Atomic.Var<StringPool> pool = db.<Atomic.Var<StringPool>>get(".keyPool");
		if (pool != null) {
			return pool.get();
		} else {
			return new StringPoolCASImpl(fixedCapacity);
		}
	}

	private static StringPool createLiteralPool(DB db) {
		Atomic.Var<StringPool> pool = db.<Atomic.Var<StringPool>>get(".literalPool");
		if (pool != null) {
			return pool.get();
		} else {
			return new StringPoolCASImpl(Integer.MAX_VALUE);
		}
	}

	private static IdFactory createIDFactory(DB db) {
		Atomic.Var<IdFactory> idFactory = db.<Atomic.Var<IdFactory>>get(".idFactory");
		if (idFactory != null) {
			return idFactory.get();
		} else {
			return new IdFactorySeq(getDefaultKeyPoolShift());
		}
	}

	/**
	 * @return underlying BTree as {@link NavigableMap}
	 */
	final NavigableMap<Long, ExternalAttribute> getMapStorage() {
		return attributeStorage;
	}

	/**
	 * Returns the composite key of the attribute and the owner. Assumes that
	 * the internal key of the attribute is already added to the pool.
	 * 
	 * @param owner
	 * @param attributekey
	 * @return the composite key
	 */
	static Long getCompositeKey(ExternalAttributable owner, int attributekey) {
		return owner.getExternalId() + attributekey;
	}

	static int getInternalAttributeKey(ExternalAttributable owner, long compositeKey) {
		return (int) (compositeKey - owner.getExternalId());
	}

	/**
	 * Returns the internal key of the attribute with the supplied
	 * {@link String} key. In case no such attribute is know, NULL is returned.
	 * 
	 * @param owner
	 * @param attributeKey
	 * @return the composite key or NULL is no such key exists
	 */
	final Integer getInternalAttributeKey(ExternalAttributable owner, String attributeKey) {
		return keyPool.getIndex(attributeKey);
	}

	final long lowestCompositeKey(ExternalAttributable owner) {
		// Reversed for data pump
		return owner.getExternalId() + (getAttributeKeyPool().getCapacity() - 1);
	}

	final long highestCompositeKey(ExternalAttributable owner) {
		// Reversed for data pump
		return owner.getExternalId();
	}

	@Override
	protected final XAttributeMap createAttributeMap(ExternalAttributable attributable) {
		return new ExternalAttributeMapCaching(attributable, new MapDBAttributeMapBTreeStore(attributable, this));
	}

	// data pump methods

	@Override
	public PumpService startPump() {
		pumpService = new PumpServiceImpl(builder);
		return pumpService;
	}

	@Override
	public PumpService getPumpService() {
		return pumpService;
	}

	@Override
	public boolean isPumping() {
		return pumpService != null;
	}

	@Override
	public final StringPool getAttributeKeyPool() {
		return keyPool;
	}

	@Override
	public StringPool getLiteralPool() {
		return literalPool;
	}

	@Override
	public final IdFactory getIdFactory() {
		return idFactory;
	}

	@Override
	public final void commit() {
		if (isPumping()) {
			throw new XESLiteException("Cannot be used during data pump!");
		}

		// Only does a sync
		db.commit();
	}

	@Override
	public void dispose() {
		if (isPumping()) {
			throw new XESLiteException("Cannot be used during data pump!");
		}

		db.close();
	}

	@Override
	public void saveLogStructure(XLog log) {
		MapDBLogSerializer serializer = new MapDBLogSerializer(null);
		db.atomicVarCreate(".logStore", log, new Serializer.CompressionWrapper<>(serializer));
		db.atomicVarCreate(".literalPool", getLiteralPool(), null);
		db.atomicVarCreate(".keyPool", getAttributeKeyPool(), null);
		db.atomicVarCreate(".idFactory", getIdFactory(), null);
	}

	@Override
	public XLog loadLogStructure(XFactoryExternalStore factory) {
		MapDBLogSerializer serializer = new MapDBLogSerializer(factory);
		return db.atomicVar(".logStore", new Serializer.CompressionWrapper<>(serializer)).get();
	}

}