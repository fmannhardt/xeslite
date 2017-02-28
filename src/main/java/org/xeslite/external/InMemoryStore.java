package org.xeslite.external;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.id.XID;
import org.deckfour.xes.model.XAttributable;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeBoolean;
import org.deckfour.xes.model.XAttributeContinuous;
import org.deckfour.xes.model.XAttributeDiscrete;
import org.deckfour.xes.model.XAttributeID;
import org.deckfour.xes.model.XAttributeLiteral;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XAttributeTimestamp;
import org.deckfour.xes.model.XLog;
import org.xeslite.common.XESLiteException;
import org.xeslite.external.XAbstractCompressedList.Compressor;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import net.jpountz.lz4.LZ4Factory;

/**
 * {@link ExternalStore} implementation using on-heap {@link ByteBuffer} blocks
 * organized in a columnar format. Assuming that the stored {@link XAttribute}
 * have consistent types and extensions.
 * 
 * @author F. Mannhardt
 * 
 */
public final class InMemoryStore extends ExternalStoreAbstract {

	static final class Store implements Iterable<AttributeStorage> {

		private static final AttributeStorage INSERTING = new AttributeStorage(-1, XAttributeLiteral.class, null, null);

		private final NonBlockingHashMapLong<AttributeStorage> attributes;
		private final int blockShift;
		private final int initialBlocks;

		Store(int blockShift, int initialBlocks) {
			super();
			this.blockShift = blockShift;
			this.initialBlocks = initialBlocks;
			this.attributes = new NonBlockingHashMapLong<>(false);
		}

		public AttributeStorage getStorage(int attributeKey) {
			return attributes.get(attributeKey);
		}

		public AttributeStorage getStorage(int attributeKey, Class<? extends XAttribute> attributeClass,
				XExtension extension) {
			// Check if block exists
			AttributeStorage storage = attributes.get(attributeKey);

			if (storage != null) {
				while (storage == INSERTING) {
					LockSupport.parkNanos(10L);
					storage = attributes.get(attributeKey);
				}
				return storage;
			} else {
				// No yet present, insert a stub value as we don't want to waste indices
				storage = attributes.putIfAbsent(attributeKey, INSERTING);
				if (storage == null) {
					// We reserved the spot and, therefore, the following operations are executed atomically
					// Take care that this block never fails, otherwise other threads trying the same put starve					
					storage = new AttributeStorage(attributeKey, attributeClass, extension,
							new HeapVolume(initialBlocks, blockShift, Mode.getMode(attributeClass)));
					attributes.put(attributeKey, storage);
					return storage;
				} else {
					// Another thread reserved the sport, wait until insert is visible to us
					while (storage == INSERTING) {
						LockSupport.parkNanos(10L);
						storage = attributes.get(attributeKey);
					}
					return storage;
				}
			}
		}

		public Iterator<AttributeStorage> iterator() {
			return attributes.values().iterator();
		}

	}

	abstract static class AbstractEntry {

		private long meta;
		private long position;

		public long getPosition() {
			return position;
		}

		public long getNestedId() {
			return meta;
		}

		public void setMeta(long meta) {
			this.meta = meta;
		}

		public void setPosition(long position) {
			this.position = position;
		}
	}

	static final class LongEntry extends AbstractEntry {

		private long value;

		public long getValue() {
			return value;
		}

		public void setValue(long value) {
			this.value = value;
		}

	}

	static final class IntEntry extends AbstractEntry {

		private int value;

		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

	}

	static final class BooleanEntry extends AbstractEntry {

		private boolean value;

		public boolean isValue() {
			return value;
		}

		public void setValue(boolean value) {
			this.value = value;
		}

	}

	static final class UUIDEntry extends AbstractEntry {

		private UUID value;

		public UUID getValue() {
			return value;
		}

		public void setValue(UUID value) {
			this.value = value;
		}

	}

	private static final Compressor COMPRESSOR = new Compressor() {

		private final LZ4Factory factory = LZ4Factory.fastestJavaInstance();

		public int decompress(byte[] compressedData, byte[] uncompressedBuffer, int srcOff, int destOff, int destLen) {
			return factory.fastDecompressor().decompress(compressedData, srcOff, uncompressedBuffer, destOff, destLen);
		}

		public int compress(byte[] data, int srcOff, int srcLen, byte[] dest, int destOff) {
			return factory.fastCompressor().compress(data, srcOff, srcLen, dest, destOff);
		}

		public int maxCompressedSize(int length) {
			return factory.fastCompressor().maxCompressedLength(length);
		}

		public byte[] decompress(byte[] compressedData, int uncompressedSize) {
			return factory.fastDecompressor().decompress(compressedData, uncompressedSize);
		}

		public byte[] compress(byte[] data) {
			return factory.fastCompressor().compress(data);
		}

	};

	enum Mode {
		BOOLEAN(1, 0), INT(4 * 8, 2), LONG(8 * 8, 3), UUID(16 * 8, 4);

		private final int numBits;
		private final int posShift;

		private Mode(int numBits, int posShift) {
			this.numBits = numBits;
			this.posShift = posShift;
		}

		public int getNumBits() {
			return numBits;
		}

		public int getNumBytes(int blockSize) {
			int numBitsForValues = getNumBits() * blockSize;
			return numBitsForValues / 8;
		}

		public int getNumFlagBytes(int blockSize) {
			return blockSize >> 2; // DIV 4
		}

		public int shiftPosition(int positionInBlock) {
			return positionInBlock << posShift;
		}

		public static Mode getMode(Class<? extends XAttribute> attributeClass) {
			if (XAttributeLiteral.class.isAssignableFrom(attributeClass)) {
				return Mode.INT;
			} else if (XAttributeDiscrete.class.isAssignableFrom(attributeClass)) {
				return Mode.LONG;
			} else if (XAttributeContinuous.class.isAssignableFrom(attributeClass)) {
				return Mode.LONG;
			} else if (XAttributeTimestamp.class.isAssignableFrom(attributeClass)) {
				return Mode.LONG;
			} else if (XAttributeBoolean.class.isAssignableFrom(attributeClass)) {
				return Mode.BOOLEAN;
			} else if (XAttributeID.class.isAssignableFrom(attributeClass)) {
				return Mode.UUID;
			} else {
				return Mode.INT;
			}
		}

	}

	interface Block {

		boolean hasExistsFlag(int position);

		void setExistsFlag(int position, boolean val);

		boolean hasNestedFlag(int positionInBlock);

		void setNestedFlag(int position, boolean val);

		byte get(int pos);

		int getInt(int pos);

		long getLong(int pos);

		void put(int pos, byte val);

		void putInt(int pos, int val);

		void putLong(int pos, long val);

		void decompress();

		void compress();

	}

	static final class HeapBlock implements Block {

		private byte[] buffer;
		private final int blockSize;
		private final Mode mode;

		HeapBlock(int blockSize, Mode mode) {
			super();
			this.blockSize = blockSize;
			this.mode = mode;
			this.buffer = new byte[capacity(blockSize, mode)];
		}

		private static int capacity(int blockSize, Mode mode) {
			return mode.getNumBytes(blockSize) + mode.getNumFlagBytes(blockSize) + 1;
		}

		private boolean getFlag(int position, int flagIndex) {
			decompress();
			int valuesSize = mode.getNumBytes(blockSize);
			int byteIndex = getFlagByte(position, valuesSize) + 1;
			int bitIndex = getFlagBit(position, flagIndex);
			byte b = buffer[byteIndex];
			return 0 != (b & (1 << bitIndex)); // and with shifted mask 
		}

		private void setFlag(int position, int flagIndex, boolean val) {
			decompress();
			int valuesSize = mode.getNumBytes(blockSize);
			int byteIndex = getFlagByte(position, valuesSize) + 1;
			int bitIndex = getFlagBit(position, flagIndex);
			byte b = buffer[byteIndex];
			if (val) {
				b |= (1 << bitIndex);
			} else {
				b &= ~(1 << bitIndex);
			}
			buffer[byteIndex] = b;
		}

		private static int getFlagByte(int position, int valuesSize) {
			return valuesSize + (position >> 2); // 2 bits per entry -> 4 entries per byte -> DIV 4 -> shift 2
		}

		private static int getFlagBit(int position, int flagIndex) {
			return ((position & 3) << 1) + flagIndex; // 2 bits per entry = ((p MOD 4) MUL 2) + index
		}

		@Override
		public boolean hasExistsFlag(int position) {
			return getFlag(position, 0);
		}

		@Override
		public void setExistsFlag(int position, boolean val) {
			setFlag(position, 0, val);
		}

		@Override
		public boolean hasNestedFlag(int position) {
			return getFlag(position, 1);
		}

		@Override
		public void setNestedFlag(int position, boolean val) {
			setFlag(position, 1, val);
		}

		@Override
		public byte get(int pos) {
			decompress();
			return buffer[pos + 1];
		}

		@Override
		public int getInt(int pos) {
			decompress();
			return Ints.fromBytes(buffer[pos + 1], buffer[pos + 2], buffer[pos + 3], buffer[pos + 4]);
		}

		@Override
		public long getLong(int pos) {
			decompress();
			return Longs.fromBytes(buffer[pos + 1], buffer[pos + 2], buffer[pos + 3], buffer[pos + 4], buffer[pos + 5],
					buffer[pos + 6], buffer[pos + 7], buffer[pos + 8]);
		}

		@Override
		public void put(int pos, byte val) {
			decompress();
			buffer[pos + 1] = val;
		}

		@Override
		public void putInt(int pos, int val) {
			decompress();
			buffer[pos + 1] = (byte) (val >> 24);
			buffer[pos + 2] = (byte) (val >> 16);
			buffer[pos + 3] = (byte) (val >> 8);
			buffer[pos + 4] = (byte) (val >> 0);
		}

		@Override
		public void putLong(int pos, long val) {
			decompress();
			buffer[pos + 1] = (byte) (val >> 56);
			buffer[pos + 2] = (byte) (val >> 48);
			buffer[pos + 3] = (byte) (val >> 40);
			buffer[pos + 4] = (byte) (val >> 32);
			buffer[pos + 5] = (byte) (val >> 24);
			buffer[pos + 6] = (byte) (val >> 16);
			buffer[pos + 7] = (byte) (val >> 8);
			buffer[pos + 8] = (byte) val;
		}

		public boolean isCompressed() {
			return buffer[0] == 1;
		}

		@Override
		public void compress() {
			if (!isCompressed()) {
				byte[] compressed = new byte[1 + COMPRESSOR.maxCompressedSize(buffer.length - 1)];
				compressed[0] = (byte) 1;
				int compressedLength = COMPRESSOR.compress(buffer, 1, buffer.length - 1, compressed, 1);
				buffer = Arrays.copyOf(compressed, compressedLength + 1);
			}
		}

		@Override
		public void decompress() {
			if (isCompressed()) {
				int decompressedSize = capacity(blockSize, mode);
				byte[] decompressed = new byte[decompressedSize];
				COMPRESSOR.decompress(buffer, decompressed, 1, 1, decompressedSize - 1); //skip first byte
				decompressed[0] = (byte) 0;
				buffer = decompressed;
			}
		}

	}

	interface Volume {

		boolean hasValue(long position);

		boolean hasNested(long position);

		int getInt(long position);

		long getLong(long position);

		UUID getUUID(long position);

		boolean getBoolean(long position);

		void putInt(long position, int val, long nestedId);

		void putInts(Iterable<IntEntry> values);

		void putLong(long position, long val, long nestedId);

		void putLongs(Iterable<LongEntry> values);

		void putUUID(long position, UUID val, long nestedId);

		void putUUIDs(Iterable<UUIDEntry> values);

		void putBoolean(long position, boolean val, long nestedId);

		void putBooleans(Iterable<BooleanEntry> values);

		void remove(long position);

		void compressStorage();

		void decompressStorage();

		void compressBlock(long position);

		void decompressBlock(long position);

		/**
		 * Resizes the underlying storage to the exact size needed (not
		 * thread-safe, needs to be synchronized externally)
		 */
		void trimToSize();

	}

	static final class HeapVolume implements Volume {

		private final Mode mode;

		private final int blockShift;
		private volatile Block[] storage;

		HeapVolume(int initalBlocks, int blockShift, Mode mode) {
			super();
			this.blockShift = blockShift;
			this.mode = mode;
			this.storage = new Block[initalBlocks];
			for (int i = 0; i < initalBlocks; i++) {
				storage[i] = new HeapBlock(1 << blockShift, mode);
			}
		}

		private int getBlockMask() {
			return getBlockSize() - 1;
		}

		private int getBlockSize() {
			return 1 << blockShift;
		}

		private int getPositionInBlock(long position) {
			return (int) (position & getBlockMask());
		}

		private int getBlockIndex(long position) {
			return (int) (position >> blockShift);
		}

		private Block getBlock(int blockIndex) {
			if (blockIndex < storage.length) {
				Block block = getStorage()[blockIndex];
				if (block != null) {
					return block;
				}
			}
			return null;
		}

		private final Object growLock = new Object();

		private Block getOrCreateBlock(int blockIndex) {
			// read first
			Block block = getBlock(blockIndex);
			if (block != null) {
				return block;
			}

			// Otherwise grow copy on write
			synchronized (growLock) {
				// Check whether some other thread grew the array 
				block = getBlock(blockIndex);
				if (block != null) {
					return block;
				}

				// grow array
				final Block[] oldStorage = getStorage();
				int oldLength = oldStorage.length;
				int minLength = blockIndex + 1;
				int newLength = Math.max(minLength, (int) Math.ceil(oldLength * 1.5));
				final Block[] newStorage = Arrays.copyOf(oldStorage, Math.min(Integer.MAX_VALUE - 8, newLength));

				// allocate blocks
				for (int i = oldLength; i < newStorage.length; i++) {
					newStorage[i] = new HeapBlock(getBlockSize(), mode);
				}

				setStorage(newStorage); // volatile ensures visibility
				return newStorage[blockIndex];
			}
		}

		private void setStorage(Block[] storage) {
			this.storage = storage;
		}

		private Block[] getStorage() {
			return storage;
		}

		@Override
		public void trimToSize() {
			final Block[] oldStorage = getStorage();
			int lastFilledBlock = findLastFilledBlock(oldStorage);
			final Block[] newStorage = Arrays.copyOf(oldStorage, lastFilledBlock + 1);
			setStorage(newStorage); // volatile ensures visibility
		}

		private int findLastFilledBlock(final Block[] oldStorage) {
			for (int i = oldStorage.length - 1; i >= 0; i--) {
				Block block = oldStorage[i];
				for (int j = getBlockSize() - 1; j >= 0; j--) {
					if (block.hasExistsFlag(j)) {
						return i;
					}
				}
			}
			return 0;
		}

		@Override
		public void compressStorage() {
			for (Block block : storage) {
				synchronized (block) {
					block.compress();
				}
			}
		}

		@Override
		public void decompressStorage() {
			for (Block block : storage) {
				synchronized (block) {
					block.decompress();
				}
			}
		}

		@Override
		public void compressBlock(long position) {
			Block block = getOrCreateBlock(getBlockIndex(position));
			synchronized (block) {
				block.compress();
			}
		}

		@Override
		public void decompressBlock(long position) {
			Block block = getOrCreateBlock(getBlockIndex(position));
			synchronized (block) {
				block.decompress();
			}
		}

		//************* GET Methods *************

		@Override
		public boolean hasValue(long position) {
			Block block = getBlock(getBlockIndex(position));
			if (block != null) {
				synchronized (block) {
					return block.hasExistsFlag(getPositionInBlock(position));
				}
			} else {
				return false;
			}
		}

		@Override
		public boolean hasNested(long position) {
			Block block = getBlock(getBlockIndex(position));
			if (block != null) {
				synchronized (block) {
					return block.hasNestedFlag(getPositionInBlock(position));
				}
			} else {
				return false;
			}
		}

		@Override
		public boolean getBoolean(long position) {
			Block block = getOrCreateBlock(getBlockIndex(position));
			synchronized (block) {
				int localPosition = getPositionInBlock(position);
				int byteIndex = (localPosition >> 3); // DIV 8
				int bitIndex = localPosition & 7; // MOD 8
				byte b = block.get(byteIndex);
				return 0 != (b & (1 << bitIndex)); // AND with shifted mask 
			}
		}

		@Override
		public UUID getUUID(long position) {
			Block block = getOrCreateBlock(getBlockIndex(position));
			synchronized (block) {
				int pos1 = mode.shiftPosition(getPositionInBlock(position));
				int pos2 = pos1 + 8;
				return new UUID(block.getLong(pos1), block.getLong(pos2));
			}
		}

		@Override
		public long getLong(long position) {
			Block block = getOrCreateBlock(getBlockIndex(position));
			synchronized (block) {
				return block.getLong(mode.shiftPosition(getPositionInBlock(position)));
			}
		}

		@Override
		public int getInt(long position) {
			Block block = getOrCreateBlock(getBlockIndex(position));
			synchronized (block) {
				return block.getInt(mode.shiftPosition(getPositionInBlock(position)));
			}
		}

		//***************** PUT methods *************

		private void setFlags(Block block, long nestedId, int positionInBlock) {
			block.setExistsFlag(positionInBlock, true);
			block.setNestedFlag(positionInBlock, nestedId != -1);
		}

		private void putIntWithBlock(long position, int val, Block block, long nestedId) {
			int positionInBlock = getPositionInBlock(position);
			block.putInt(mode.shiftPosition(positionInBlock), val);
			setFlags(block, nestedId, positionInBlock);
		}

		private void putLongWithBlock(long position, long val, Block block, long nestedId) {
			int positionInBlock = getPositionInBlock(position);
			block.putLong(mode.shiftPosition(positionInBlock), val);
			setFlags(block, nestedId, positionInBlock);
		}

		private void putBooleanWithBlock(long position, boolean val, Block block, long nestedId) {
			int positionInBlock = getPositionInBlock(position);
			int byteIndex = (positionInBlock >> 3); // divide by 8
			int bitIndex = positionInBlock & 7; // mod 8
			byte b = block.get(byteIndex);
			if (val) {
				b |= (1 << bitIndex); // or with shifted mask	
			} else {
				b &= ~(1 << bitIndex); // or with shifted mask
			}
			block.put(byteIndex, b);
			setFlags(block, nestedId, positionInBlock);
		}

		private void putUUIDWithBlock(long position, UUID val, Block block, long nestedId) {
			int positionInBlock = getPositionInBlock(position);
			int pos1 = mode.shiftPosition(positionInBlock);
			int pos2 = pos1 + 8;
			block.putLong(pos1, val.getMostSignificantBits());
			block.putLong(pos2, val.getLeastSignificantBits());
			setFlags(block, nestedId, positionInBlock);
		}

		//***************** PUT API method *************

		@Override
		public void remove(long position) {
			Block block = getOrCreateBlock(getBlockIndex(position));
			synchronized (block) {
				int positionInBlock = getPositionInBlock(position);
				block.setExistsFlag(positionInBlock, false);
				block.setNestedFlag(positionInBlock, false);
			}
		}

		@Override
		public void putBoolean(long position, boolean val, long nestedId) {
			Block block = getOrCreateBlock(getBlockIndex(position));
			synchronized (block) {
				putBooleanWithBlock(position, val, block, nestedId);
			}
		}

		@Override
		public void putBooleans(Iterable<BooleanEntry> values) {
			PeekingIterator<BooleanEntry> iterator = Iterators.peekingIterator(values.iterator());
			if (iterator.hasNext()) {
				long position = iterator.peek().getPosition();
				while (iterator.hasNext()) {
					int currentBlockIndex = getBlockIndex(position);
					Block block = getOrCreateBlock(currentBlockIndex);
					while (iterator.hasNext()) {
						position = iterator.peek().getPosition();
						if (getBlockIndex(position) == currentBlockIndex) {
							BooleanEntry val = iterator.next();
							putBooleanWithBlock(position, val.isValue(), block, val.getNestedId());
						} else {
							break;
						}
					}
				}
			}
		}

		@Override
		public void putUUID(long position, UUID val, long nestedId) {
			Block block = getOrCreateBlock(getBlockIndex(position));
			synchronized (block) {
				putUUIDWithBlock(position, val, block, nestedId);
			}
		}

		@Override
		public void putUUIDs(Iterable<UUIDEntry> values) {
			PeekingIterator<UUIDEntry> iterator = Iterators.peekingIterator(values.iterator());
			if (iterator.hasNext()) {
				long position = iterator.peek().getPosition();
				while (iterator.hasNext()) {
					int currentBlockIndex = getBlockIndex(position);
					Block block = getOrCreateBlock(currentBlockIndex);
					while (iterator.hasNext()) {
						position = iterator.peek().getPosition();
						if (getBlockIndex(position) == currentBlockIndex) {
							UUIDEntry val = iterator.next();
							putUUIDWithBlock(position, val.getValue(), block, val.getNestedId());
						} else {
							break;
						}
					}
				}
			}
		}

		@Override
		public void putLong(long position, long val, long nestedId) {
			Block block = getOrCreateBlock(getBlockIndex(position));
			synchronized (block) {
				putLongWithBlock(position, val, block, nestedId);
			}
		}

		@Override
		public void putLongs(Iterable<LongEntry> values) {
			PeekingIterator<LongEntry> iterator = Iterators.peekingIterator(values.iterator());
			if (iterator.hasNext()) {
				long position = iterator.peek().getPosition();
				while (iterator.hasNext()) {
					int currentBlockIndex = getBlockIndex(position);
					Block block = getOrCreateBlock(currentBlockIndex);
					while (iterator.hasNext()) {
						position = iterator.peek().getPosition();
						if (getBlockIndex(position) == currentBlockIndex) {
							LongEntry val = iterator.next();
							putLongWithBlock(position, val.getValue(), block, val.getNestedId());
						} else {
							break;
						}
					}
				}
			}
		}

		@Override
		public void putInt(long position, int val, long nestedId) {
			Block block = getOrCreateBlock(getBlockIndex(position));
			synchronized (block) {
				putIntWithBlock(position, val, block, nestedId);
			}
		}

		@Override
		public void putInts(Iterable<IntEntry> values) {
			PeekingIterator<IntEntry> iterator = Iterators.peekingIterator(values.iterator());
			if (iterator.hasNext()) {
				long position = iterator.peek().getPosition();
				while (iterator.hasNext()) {
					int currentBlockIndex = getBlockIndex(position);
					Block block = getOrCreateBlock(currentBlockIndex);
					while (iterator.hasNext()) {
						position = iterator.peek().getPosition();
						if (getBlockIndex(position) == currentBlockIndex) {
							IntEntry val = iterator.next();
							putIntWithBlock(position, val.getValue(), block, val.getNestedId());
						} else {
							break;
						}
					}
				}
			}
		}

	}

	static final class AttributeStorage {

		private final int attributeKey;
		private final Class<? extends XAttribute> type;
		private final XExtension extension;

		private final Volume volume;

		public AttributeStorage(int attributeKey, Class<? extends XAttribute> type, XExtension extension,
				Volume volume) {
			this.attributeKey = attributeKey;
			this.type = type;
			this.extension = extension;
			this.volume = volume;
		}

		public Volume getVolume() {
			return volume;
		}

		public Class<? extends XAttribute> getType() {
			return type;
		}

		public XExtension getExtension() {
			return extension;
		}

		public int getAttributeKey() {
			return attributeKey;
		}

	}

	static final class AttributeStoreImpl implements AttributeStore<ExternalAttribute> {

		private final StringPool literalPool;
		private final Store store;
		private final Store nestedStore;

		AttributeStoreImpl(int blockSize, int initialBlocks, StringPool literalPool) {
			this.store = new Store((int) Math.log(blockSize), initialBlocks);
			this.nestedStore = new Store((int) Math.log(blockSize), 0);
			this.literalPool = literalPool;
		}

		private AttributeStorage getAttributeStorage(int attributeKey, ExternalAttribute attribute) {
			return store.getStorage(attributeKey, attribute.getClass(), attribute.getExtension());
		}

		private AttributeStorage getStorage(int attributeKey) {
			return store.getStorage(attributeKey);
		}

		@Override
		public ExternalAttribute getValue(int attributeKey, long objectKey) {
			AttributeStorage storage = getStorage(attributeKey);
			if (storage == null) {
				return null;
			} else {
				if (storage.getVolume().hasValue(objectKey)) {
					return retrieveValue(objectKey, storage);
				} else {
					return null;
				}
			}
		}

		protected ExternalAttribute retrieveValue(long objectKey, AttributeStorage storage) {
			Class<? extends XAttribute> type = storage.getType();
			Volume vol = storage.getVolume();
			ExternalAttribute rawAttribute = retrieveRawValue(objectKey, storage, type, vol);
			if (vol.hasNested(objectKey)) {
				// retrieve external id of the nested attributes
				AttributeStorage nestedStorage = nestedStore.getStorage(storage.getAttributeKey());
				Volume nestedVol = nestedStorage.getVolume();
				rawAttribute.setExternalId(nestedVol.getLong(objectKey));
			}
			return rawAttribute;
		}

		private ExternalAttribute retrieveRawValue(long objectKey, AttributeStorage storage,
				Class<? extends XAttribute> type, Volume vol) {
			if (XAttributeLiteral.class.isAssignableFrom(type)) {
				return new XAttributeLiteralExternalImpl(storage.getAttributeKey(),
						literalPool.getValue(vol.getInt(objectKey)), storage.getExtension(), null, null);
			} else if (XAttributeDiscrete.class.isAssignableFrom(type)) {
				return new XAttributeDiscreteExternalImpl(storage.getAttributeKey(), vol.getLong(objectKey),
						storage.getExtension(), null, null);
			} else if (XAttributeContinuous.class.isAssignableFrom(type)) {
				return new XAttributeContinuousExternalImpl(storage.getAttributeKey(),
						Double.longBitsToDouble(vol.getLong(objectKey)), storage.getExtension(), null, null);
			} else if (XAttributeTimestamp.class.isAssignableFrom(type)) {
				return new XAttributeTimestampExternalImpl(storage.getAttributeKey(), vol.getLong(objectKey),
						storage.getExtension(), null, null);
			} else if (XAttributeBoolean.class.isAssignableFrom(type)) {
				return new XAttributeBooleanExternalImpl(storage.getAttributeKey(), vol.getBoolean(objectKey),
						storage.getExtension(), null, null);
			} else if (XAttributeID.class.isAssignableFrom(type)) {
				return new XAttributeIDExternalImpl(storage.getAttributeKey(), new XID(vol.getUUID(objectKey)),
						storage.getExtension(), null, null);
			} else {
				throw new UnsupportedOperationException();
			}
		}

		@Override
		public ExternalAttribute putValue(int attributeKey, long objectKey, ExternalAttribute value) {
			AttributeStorage storage = getAttributeStorage(attributeKey, value);
			ExternalAttribute oldValue = null;
			if (storage.getVolume().hasValue(objectKey)) {
				oldValue = retrieveValue(objectKey, storage);
			}
			if (value != null) {
				storeValue(attributeKey, objectKey, storage, value);
			} else {
				storage.getVolume().remove(objectKey);
			}
			return oldValue;
		}

		@Override
		public void setValue(int attributeKey, long objectKey, ExternalAttribute value) {
			storeValue(attributeKey, objectKey, getAttributeStorage(attributeKey, value), value);
		}

		@Override
		public void setValues(int attributeKey, Iterable<ExternalAttribute> values) {
			assert Iterables.size(values) > 0;
			AttributeStorage storage = getAttributeStorage(attributeKey, Iterables.getFirst(values, null));
			Class<? extends XAttribute> type = storage.getType();
			Volume vol = storage.getVolume();
			if (XAttributeLiteral.class.isAssignableFrom(type)) {
				final IntEntry entry = new IntEntry();
				vol.putInts(Iterables.transform(values, new Function<ExternalAttribute, IntEntry>() {

					public IntEntry apply(final ExternalAttribute a) {
						prepareEntry(entry, a);
						entry.setValue(literalPool.put(getAsString(a)));
						return entry;
					}

				}));
			} else if (XAttributeDiscrete.class.isAssignableFrom(type)) {
				final LongEntry entry = new LongEntry();
				vol.putLongs(Iterables.transform(values, new Function<ExternalAttribute, LongEntry>() {

					public LongEntry apply(final ExternalAttribute a) {
						prepareEntry(entry, a);
						entry.setValue(getAsLong(a));
						return entry;
					}

				}));
			} else if (XAttributeContinuous.class.isAssignableFrom(type)) {
				final LongEntry entry = new LongEntry();
				vol.putLongs(Iterables.transform(values, new Function<ExternalAttribute, LongEntry>() {

					public LongEntry apply(final ExternalAttribute a) {
						prepareEntry(entry, a);
						entry.setValue(Double.doubleToLongBits(getAsDouble(a)));
						return entry;
					}

				}));
			} else if (XAttributeTimestamp.class.isAssignableFrom(type)) {
				final LongEntry entry = new LongEntry();
				vol.putLongs(Iterables.transform(values, new Function<ExternalAttribute, LongEntry>() {

					public LongEntry apply(final ExternalAttribute a) {
						prepareEntry(entry, a);
						entry.setValue(getAsLong(a));
						return entry;
					}

				}));
			} else if (XAttributeBoolean.class.isAssignableFrom(type)) {
				final BooleanEntry entry = new BooleanEntry();
				vol.putBooleans(Iterables.transform(values, new Function<ExternalAttribute, BooleanEntry>() {

					public BooleanEntry apply(final ExternalAttribute a) {
						prepareEntry(entry, a);
						entry.setValue(((XAttributeBoolean) a).getValue());
						return entry;
					}

				}));
			} else if (XAttributeID.class.isAssignableFrom(type)) {
				final UUIDEntry entry = new UUIDEntry();
				vol.putUUIDs(Iterables.transform(values, new Function<ExternalAttribute, UUIDEntry>() {

					public UUIDEntry apply(final ExternalAttribute a) {
						prepareEntry(entry, a);
						XID id = ((XAttributeID) a).getValue();
						UUID uuid = UUID.fromString(id.toString()); //TODO horrible workaround :)
						entry.setValue(uuid);
						return entry;
					}

				}));
			} else {
				throw new XESLiteException("Unsupported attribute type " + type.getSimpleName());
			}
			batchPutNested(attributeKey, values);
		}

		private static String getAsString(final ExternalAttribute a) {
			if (a instanceof XAttributeLiteral) {
				return ((XAttributeLiteral) a).getValue();
			} else {
				return a.toString();
			}
		}

		private static long getAsLong(final ExternalAttribute a) {
			if (a instanceof XAttributeTimestamp) {
				return ((XAttributeTimestamp) a).getValueMillis();
			} else if (a instanceof XAttributeDiscrete) {
				return ((XAttributeDiscrete) a).getValue();
			} else if (a instanceof XAttributeContinuous) {
				return (long) ((XAttributeContinuous) a).getValue();
			} else {
				throw new XESLiteException(
						"Inconsistent attribute " + a.getKey() + ": continuous instead of expected discrete.");
			}
		}

		private static double getAsDouble(final ExternalAttribute a) {
			if (a instanceof XAttributeContinuous) {
				return ((XAttributeContinuous) a).getValue();
			} else {
				if (a instanceof XAttributeTimestamp) {
					return ((XAttributeTimestamp) a).getValueMillis();
				} else if (a instanceof XAttributeDiscrete) {
					return ((XAttributeDiscrete) a).getValue();
				} else {
					throw new XESLiteException(
							"Inconsistent attribute " + a.getKey() + ": continuous instead of expected discrete.");
				}
			}
		}

		private void batchPutNested(int attributeKey, Iterable<ExternalAttribute> values) {
			for (ExternalAttribute a : values) {
				long nestedId = getNestedId(a);
				storeNested(attributeKey, a.getOwner().getExternalId(), nestedId);
			}
		}

		private static void prepareEntry(final AbstractEntry entry, final ExternalAttribute a) {
			if (a instanceof ExternalAttributable && a.hasAttributes()) {
				entry.setMeta(((ExternalAttributable) a).getExternalId());
			} else {
				entry.setMeta(-1);
			}
			entry.setPosition(a.getOwner().getExternalId());
		}

		private void storeValue(int attributeKey, long objectKey, AttributeStorage storage, ExternalAttribute a) {
			Class<? extends XAttribute> attributeClass = storage.getType();
			Volume vol = storage.getVolume();
			long nestedId = getNestedId(a);
			if (XAttributeLiteral.class.isAssignableFrom(attributeClass)) {
				vol.putInt(objectKey, literalPool.put(getAsString(a)), nestedId);
			} else if (XAttributeDiscrete.class.isAssignableFrom(attributeClass)) {
				vol.putLong(objectKey, getAsLong(a), nestedId);
			} else if (XAttributeContinuous.class.isAssignableFrom(attributeClass)) {
				vol.putLong(objectKey, Double.doubleToLongBits(getAsDouble(a)), nestedId);
			} else if (XAttributeTimestamp.class.isAssignableFrom(attributeClass)) {
				vol.putLong(objectKey, getAsLong(a), nestedId);
			} else if (XAttributeBoolean.class.isAssignableFrom(attributeClass)) {
				vol.putBoolean(objectKey, ((XAttributeBoolean) a).getValue(), nestedId);
			} else if (XAttributeID.class.isAssignableFrom(attributeClass)) {
				XID id = ((XAttributeID) a).getValue();
				UUID uuid = UUID.fromString(id.toString()); //TODO horrible workaround :)
				vol.putUUID(objectKey, uuid, nestedId);
			} else {
				throw new UnsupportedOperationException();
			}
			storeNested(attributeKey, objectKey, nestedId);
		}

		private void storeNested(int attributeKey, long objectKey, long nestedId) {
			if (nestedId != -1) {
				AttributeStorage nestedStorage = nestedStore.getStorage(attributeKey, XAttributeDiscrete.class, null);
				nestedStorage.getVolume().putLong(objectKey, nestedId, -1); // remember ID 
			}
		}

		private long getNestedId(ExternalAttribute a) {
			if (a.hasAttributes()) {
				return ((ExternalAttributable) a).getExternalId();
			} else {
				return -1;
			}
		}

		@Override
		public ExternalAttribute removeValue(int attributeKey, long objectKey) {
			return putValue(attributeKey, objectKey, null);
		}

		@Override
		public boolean hasValue(int attributeKey, long objectKey) {
			AttributeStorage meta = getStorage(attributeKey);
			if (meta != null) {
				return meta.getVolume().hasValue(objectKey);
			} else {
				return false;
			}
		}

		private final class ValueIter implements Iterator<ExternalAttribute> {

			private final long objectKey;
			private final Iterator<AttributeStorage> storageIter;

			private ExternalAttribute current;

			//only used for remove
			private int currentIndex = -1;
			private int lastIndex = -1;

			private ValueIter(Long objectKey) {
				this.objectKey = objectKey;
				this.storageIter = store.iterator();
				this.current = moveToNext();
			}

			public boolean hasNext() {
				return current != null;
			}

			private ExternalAttribute moveToNext() {
				while (storageIter.hasNext()) {
					AttributeStorage nextStorage = storageIter.next();
					if (nextStorage.getVolume().hasValue(objectKey)) {
						ExternalAttribute value = retrieveValue(objectKey, nextStorage);
						assert value != null;
						currentIndex = nextStorage.getAttributeKey();
						value.setInternalKey(currentIndex); // need to set the key here as ExternalAttributeMap cannot know
						value.setExtension(nextStorage.getExtension());
						return value;
					}
				}
				return null;
			}

			public ExternalAttribute next() {
				ExternalAttribute entry = current;
				lastIndex = currentIndex;
				current = moveToNext();
				return entry;
			}

			public void remove() {
				if (lastIndex == -1) {
					throw new IllegalStateException();
				}
				putValue(lastIndex, objectKey, null); // calling putValue directly to avoid to retrieve old value
				lastIndex = -1;
			}

		}

		@Override
		public Iterator<ExternalAttribute> iterateValues(long objectKey) {
			return new ValueIter(objectKey);
		}

		@Override
		public void clear(long objectKey) {
			Iterator<ExternalAttribute> iter = iterateValues(objectKey);
			while (iter.hasNext()) {
				iter.remove();
			}
		}

		@Override
		public int size(long objectKey) {
			return Iterators.size(iterateValues(objectKey));
		}

		@Override
		public void startPump() {
		}

		private static final class CompressionWorker implements Runnable {

			private final WeakReference<Store> attributesRef;
			private final ScheduledExecutorService service;

			private CompressionWorker(Store attributes, ScheduledExecutorService service) {
				super();
				this.service = service;
				attributesRef = new WeakReference<Store>(attributes);
			}

			public void run() {
				Store attributes = attributesRef.get();
				if (attributes != null) {
					for (AttributeStorage meta : attributes) {
						if (!Thread.currentThread().isInterrupted()) {
							meta.getVolume().compressStorage();
						}
					}
				} else {
					service.shutdownNow();
				}
			}
		}

		private final static ThreadFactoryBuilder COMPRESSION_THREAD_BUILDER = new ThreadFactoryBuilder()
				.setDaemon(true).setNameFormat("XESLite-InMemoryStore-Compression-Daemon-Thread-%d");
		private int compressionInterval = 30;
		private ScheduledFuture<?> compressionWorker;

		@Override
		public void finishPump() {
			trimToSize();
			startCompression();
		}

		@Override
		public void startCompression() {
			if (compressionWorker == null) {
				ScheduledExecutorService service = Executors.newScheduledThreadPool(1,
						COMPRESSION_THREAD_BUILDER.build());
				compressionWorker = service.scheduleAtFixedRate(new CompressionWorker(store, service), 0,
						compressionInterval, TimeUnit.SECONDS);
			}
		}

		@Override
		public void stopCompression() {
			if (compressionWorker != null) {
				compressionWorker.cancel(true);
			}
		}

		public void trimToSize() {
			for (AttributeStorage storage : store) {
				storage.getVolume().trimToSize();
			}
			for (AttributeStorage storage : nestedStore) {
				storage.getVolume().trimToSize();
			}
		}

		public Map<Integer, Class<?>> getAttributeTypes() {
			Map<Integer, Class<?>> attributeTypes = new HashMap<>();
			for (AttributeStorage storage : store) {
				attributeTypes.put(storage.getAttributeKey(), convertType(storage.getType()));
			}
			return attributeTypes;
		}

		private Class<?> convertType(Class<? extends XAttribute> type) {
			if (XAttributeLiteral.class.isAssignableFrom(type)) {
				return String.class;
			} else if (XAttributeBoolean.class.isAssignableFrom(type)) {
				return Boolean.class;
			} else if (XAttributeContinuous.class.isAssignableFrom(type)) {
				return Double.class;
			} else if (XAttributeDiscrete.class.isAssignableFrom(type)) {
				return Long.class;
			} else if (XAttributeTimestamp.class.isAssignableFrom(type)) {
				return Date.class;
			} else if (XAttributeID.class.isAssignableFrom(type)) {
				return XID.class;
			} else {
				throw new IllegalArgumentException("Unexpected attribute type!");
			}
		}

	}

	interface AttributeStore<V> {

		boolean hasValue(int attributeKey, long objectKey);

		V getValue(int attributeKey, long objectKey);

		V putValue(int attributeKey, long objectKey, V value);

		void setValue(int attributeKey, long objectKey, V value);

		void setValues(int key, Iterable<V> values);

		V removeValue(int attributeKey, long objectKey);

		void clear(long objectKey);

		int size(long objectKey);

		Iterator<V> iterateValues(long objectKey);

		void startPump();

		void finishPump();

		void startCompression();

		void stopCompression();

		void trimToSize();

		Map<Integer, Class<?>> getAttributeTypes();

	}

	private static final int BLOCK_SIZE = 16384;
	private static final int INITITAL_BLOCK_COUNT = 8;

	private final AttributeStore<ExternalAttribute> store;

	private final IdFactory idFactory;
	private final StringPool keyPool;
	private final StringPool literalPool;

	private PumpService pumpService;

	public InMemoryStore() {
		super();
		this.idFactory = new IdFactorySeq(0);
		this.keyPool = new StringPoolCASImpl(Integer.MAX_VALUE);
		this.literalPool = new StringPoolCASImpl(Integer.MAX_VALUE);
		this.store = new AttributeStoreImpl(BLOCK_SIZE, INITITAL_BLOCK_COUNT, literalPool);
	}

	final AttributeStore<ExternalAttribute> getAttributeStore() {
		return store;
	}

	@Override
	protected final XAttributeMap createAttributeMap(ExternalAttributable attributable) {
		return new ExternalAttributeMapCaching(attributable, new InMemoryAttributeMap(attributable, this));
	}

	/**
	 * Pump is not thread-safe!
	 */
	private final class ByteStorePumpServiceImpl implements PumpService {

		private int bufferAttributables = 0;
		private ListMultimap<Integer, ExternalAttribute> buffer = ArrayListMultimap
				.<Integer, ExternalAttribute>create();

		public void pumpAttributes(XAttributable attributable, List<XAttribute> attributes) {
			if (attributable instanceof ExternalAttributable) {
				if (!attributes.isEmpty()) {
					cacheIfCacheable(attributable, attributes);
					fillBuffer(attributable, attributes);
				}
			} else {
				for (XAttribute attr : attributes) {
					attributable.getAttributes().put(attr.getKey(), attr);
				}
			}

			if (bufferAttributables++ == BLOCK_SIZE) {
				for (Entry<Integer, Collection<ExternalAttribute>> bufferPartition : buffer.asMap().entrySet()) {
					store.setValues(bufferPartition.getKey(), bufferPartition.getValue());
				}
				buffer.clear();
				bufferAttributables = 0;
			}
		}

		private void fillBuffer(XAttributable attributable, List<XAttribute> attributes) {
			ExternalAttributable owner = (ExternalAttributable) attributable;
			for (XAttribute attribute : attributes) {
				ExternalAttribute externalAttribute = XAttributeExternalImpl.convert(InMemoryStore.this, owner,
						attribute);
				buffer.put(externalAttribute.getInternalKey(), externalAttribute);
			}
		}

		private void cacheIfCacheable(XAttributable attributable, List<XAttribute> attributes) {
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
		}

		public void finishPump() throws InterruptedException {
			for (Entry<Integer, Collection<ExternalAttribute>> bufferPartition : buffer.asMap().entrySet()) {
				store.setValues(bufferPartition.getKey(), bufferPartition.getValue());
			}
			buffer = null;
			bufferAttributables = 0;
			store.finishPump();
			InMemoryStore.this.pumpService = null; // Remove reference to ourselves for GC
		}
	}

	@Override
	public PumpService startPump() {
		pumpService = new ByteStorePumpServiceImpl();
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
		getAttributeStore().trimToSize();
		getAttributeStore().startCompression();
	}

	@Override
	public void dispose() {
		getAttributeStore().stopCompression();
	}

	@Override
	public void saveLogStructure(XLog log) {
		throw new UnsupportedOperationException();
	}

	@Override
	public XLog loadLogStructure(XFactoryExternalStore factory) {
		throw new UnsupportedOperationException();
	}

	public Map<String, Class<?>> getAttributeTypes() {
		Map<Integer, Class<?>> attributeTypes = store.getAttributeTypes();
		Map<String, Class<?>> attributeTypesWithKey = new HashMap<>();
		for (Entry<Integer, Class<?>> entry : attributeTypes.entrySet()) {
			attributeTypesWithKey.put(keyPool.getValue(entry.getKey()), entry.getValue());
		}
		return attributeTypesWithKey;
	}

}