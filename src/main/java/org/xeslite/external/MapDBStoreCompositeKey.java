package org.xeslite.external;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.extension.std.XLifecycleExtension;
import org.deckfour.xes.extension.std.XTimeExtension;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XLog;
import org.mapdb.Atomic;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBException;
import org.mapdb.Serializer;
import org.xeslite.common.XESLiteException;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

/**
 * Implementation using a Object[] as key 
 * 
 * @author F. Mannhardt
 * 
 */
public final class MapDBStoreCompositeKey extends ExternalStoreAbstract {

	private static final class AttributeStoreImpl<T> implements AttributeStore<Integer, Long, T> {

		private final BTreeMap<Object[], T> attributeStore;

		public AttributeStoreImpl(DB db, Serializer<T> serializer) {
			this.attributeStore = db.treeMapCreate(".attributeStore")
					.keySerializer(new BTreeKeySerializer.Compress(new BTreeKeySerializer.ArrayKeySerializer(
							Serializer.LONG_PACKED, Serializer.INTEGER_PACKED))) //
					.valueSerializer(new Serializer.CompressionWrapper<>(serializer))//
					.nodeSize(32)//
					.make();
		}

		public T getValue(Integer attributeKey, Long objectKey) {
			return attributeStore.get(getStoreKey(objectKey, attributeKey));
		}

		private Object[] getStoreKey(Long objectKey, Integer attributeKey) {
			return new Object[] { Long.valueOf(objectKey), Integer.valueOf(attributeKey) };
		}

		public T setValue(Integer attributeKey, Long objectKey, T value) {
			return attributeStore.put(getStoreKey(objectKey, attributeKey), value);
		}

		public T removeValue(Integer attributeKey, Long objectKey) {
			return attributeStore.remove(getStoreKey(objectKey, attributeKey));
		}

		public boolean hasValue(Integer attributeKey, Long objectKey) {
			return attributeStore.containsKey(getStoreKey(objectKey, attributeKey));
		}

		public Iterator<Map.Entry<Integer, T>> iterateEntries(final Long objectKey) {
			return Iterators.transform(subMap(objectKey).entrySet().iterator(),
					new Function<Map.Entry<Object[], T>, Map.Entry<Integer, T>>() {

						public Entry<Integer, T> apply(Entry<Object[], T> entry) {
							return Maps.immutableEntry((Integer) entry.getKey()[1], entry.getValue());
						}
					});
		}

		private NavigableMap<Object[], T> subMap(final Long objectKey) {
			return attributeStore.subMap(new Object[] { objectKey }, true, new Object[] { objectKey + 1 }, false);
		}

		public int size(Long objectKey) {
			return subMap(objectKey).size();
		}

		public void clear(Long objectKey) {
			subMap(objectKey).clear();
		}

	}

	interface AttributeStore<A, O, V> {

		V getValue(A attributeKey, O objectKey);

		Iterator<Map.Entry<A, V>> iterateEntries(O objectKey);

		V setValue(A attributeKey, O objectKey, V value);

		V removeValue(A attributeKey, O objectKey);

		boolean hasValue(A attributeKey, O objectKey);

		int size(O objectKey);

		void clear(O objectKey);

	}

	public static final class Builder {

		private MapDBDatabase database;

		public Builder withDatabase(MapDBDatabase database) {
			this.database = database;
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
			return new MapDBStoreCompositeKey(this);
		}

	}

	private final DB db;
	private final MapDBAttributeSerializer serializer;

	private final AttributeStore<Integer, Long, ExternalAttribute> columnStore;

	private final IdFactory idFactory;
	private final StringPool keyPool;
	private final StringPool literalPool;

	private MapDBStoreCompositeKey(Builder builder) {
		super();
		this.db = builder.database.getDB();
		this.idFactory = createIDFactory(db);
		this.keyPool = createKeyPool(db);
		this.literalPool = createLiteralPool(db);
		this.serializer = createSerializer(literalPool, keyPool);
		this.columnStore = new AttributeStoreImpl<>(db, serializer);
	}

	private MapDBAttributeSerializer createSerializer(StringPool literalPool, StringPool keyPool) {
		Integer nameIndex = keyPool.put(XConceptExtension.KEY_NAME);
		Integer transitionIndex = keyPool.put(XLifecycleExtension.KEY_TRANSITION);
		keyPool.put(XTimeExtension.KEY_TIMESTAMP); // is not pooled as it is not literal
		Set<Integer> keysToPool = ImmutableSet.of(nameIndex, transitionIndex);
		MapDBAttributeSerializer serializer = new MapDBAttributeSerializer(literalPool, keysToPool);
		return serializer;
	}

	private static StringPool createKeyPool(DB db) {
		Atomic.Var<StringPool> pool = db.<Atomic.Var<StringPool>>get(".keyPool");
		if (pool != null) {
			return pool.get();
		} else {
			return new StringPoolCASImpl(Integer.MAX_VALUE);
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
			return new IdFactorySeq(0);
		}
	}

	final AttributeStore<Integer, Long, ExternalAttribute> getAttributeStore() {
		return columnStore;
	}

	@Override
	protected final XAttributeMap createAttributeMap(ExternalAttributable attributable) {
		return new ExternalAttributeMapCaching(attributable,
				new MapDBAttributeMapBTreeCompositeStore(attributable, this));
	}

	@Override
	public PumpService startPump() {
		throw new UnsupportedOperationException();
	}

	@Override
	public PumpService getPumpService() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isPumping() {
		return false;
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
		throw new UnsupportedOperationException();
	}

	@Override
	public XLog loadLogStructure(XFactoryExternalStore factory) {
		throw new UnsupportedOperationException();
	}

}