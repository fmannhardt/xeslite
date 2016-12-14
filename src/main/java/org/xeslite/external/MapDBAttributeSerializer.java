package org.xeslite.external;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.extension.XExtensionManager;
import org.deckfour.xes.id.XID;
import org.deckfour.xes.model.XAttributeBoolean;
import org.deckfour.xes.model.XAttributeContinuous;
import org.deckfour.xes.model.XAttributeDiscrete;
import org.deckfour.xes.model.XAttributeID;
import org.deckfour.xes.model.XAttributeLiteral;
import org.deckfour.xes.model.XAttributeTimestamp;
import org.deckfour.xes.model.XEvent;
import org.mapdb.DataIO.DataInputInternal;
import org.mapdb.DataIO.DataOutputByteArray;
import org.mapdb.Serializer;
import org.xeslite.common.XESLiteException;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

/**
 * Custom {@link Serializer} for {@link XAttributeExternalImpl}. Attributes take
 * use the following space:
 * <p>
 * <li>1 byte for their metadata;
 * <li>8 bytes (optionally) if they have nested attributes;
 * <li>1 bytes (optionally) if they have extensions;
 * <li>x bytes for their actual content.
 * 
 * @author F. Mannhardt
 * 
 */
final class MapDBAttributeSerializer extends Serializer<ExternalAttribute> {

	private static final long TIMESTAMP_REFERENCE = 946684800l;

	private static final int POOL_BITMASK = 1 << 5;
	private static final int EXTENSIONS_BITMASK = 1 << 6;
	private static final int ATTRIBUTES_BITMASK = 1 << 7;

	private static final int LIST = 8;
	private static final int CONTAINER = 7;
	private static final int ID = 6;
	private static final int TIMESTAMP = 5;
	private static final int LITERAL = 4;
	private static final int DISCRETE = 3;
	private static final int CONTINUOUS = 2;
	private static final int BOOLEAN_FALSE = 1;
	private static final int BOOLEAN_TRUE = 0;

	private final transient Map<XExtension, Integer> extensionMap = new ConcurrentHashMap<>();

	private final IntSet keysToPool;
	private final StringPool literalPool;

	MapDBAttributeSerializer(StringPool literalPool, Set<Integer> keysToPool) {
		super();
		this.literalPool = literalPool;
		this.keysToPool = new IntOpenHashSet(keysToPool);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.mapdb.Serializer#serialize(java.io.DataOutput, java.lang.Object)
	 */
	@Override
	public void serialize(DataOutput out, ExternalAttribute a) throws IOException {

		DataOutputByteArray out2 = (DataOutputByteArray) out;

		int extensionIndex = getExtensionIndex(a.getExtension());
		boolean hasExtension = extensionIndex != -1;
		boolean hasAttributes = a.hasAttributes();

		if (a instanceof XAttributeBoolean) {
			if (((XAttributeBoolean) a).getValue()) {
				out2.writeByte(addMetaData(hasAttributes, hasExtension, false, BOOLEAN_TRUE));
			} else {
				out2.writeByte(addMetaData(hasAttributes, hasExtension, false, BOOLEAN_FALSE));
			}
		} else if (a instanceof XAttributeContinuous) {
			out2.writeByte(addMetaData(hasAttributes, hasExtension, false, CONTINUOUS));
			out2.writeDouble(((XAttributeContinuous) a).getValue());
		} else if (a instanceof XAttributeDiscrete) {
			out2.writeByte(addMetaData(hasAttributes, hasExtension, false, DISCRETE));
			out2.packLong(((XAttributeDiscrete) a).getValue());
		} else if (a instanceof XAttributeTimestamp) {
			out2.writeByte(addMetaData(hasAttributes, hasExtension, false, TIMESTAMP));
			long valueMillis = ((XAttributeTimestamp) a).getValueMillis() - TIMESTAMP_REFERENCE;
			out2.packLong(valueMillis);
		} else if (a instanceof XAttributeID) {
			out2.writeByte(addMetaData(hasAttributes, hasExtension, false, ID));
			XID.write(((XAttributeID) a).getValue(), out2);
		} else if (a instanceof XAttributeContainerExternalImpl) {
			out2.writeByte(addMetaData(hasAttributes, hasExtension, false, CONTAINER));
		} else if (a instanceof XAttributeListExternalImpl) {
			out2.writeByte(addMetaData(hasAttributes, hasExtension, false, LIST));
			out2.packInt(((XAttributeListExternalImpl) a).getKeyOrder().size());
			for (String key : ((XAttributeListExternalImpl) a).getKeyOrder()) {
				out2.writeUTF(key);
			}
		} else if (a instanceof XAttributeLiteral) {
			// Literal has to be checked after List and Container!!!		
			boolean shouldPool = shouldPool(a);
			out.writeByte(addMetaData(hasAttributes, hasExtension, shouldPool, LITERAL));
			if (shouldPool) {
				out2.packInt(literalPool.put(((XAttributeLiteral) a).getValue()));
			} else {
				out2.writeUTF(((XAttributeLiteral) a).getValue());
			}
		} else {
			throw new XESLiteException("XESLite: Serialization failed for attribute " + a);
		}

		if (hasExtension) {
			if (extensionIndex > Byte.MAX_VALUE) {
				throw new XESLiteException(
						"XESLite:  oo many XExtension registered for serializer, only supporting 127.");
			}
			out2.writeByte(extensionIndex);
		}
		if (hasAttributes) {
			out2.packLong(((ExternalIdentifyable) a).getExternalId());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.mapdb.Serializer#deserialize(java.io.DataInput, int)
	 */
	@Override
	public ExternalAttribute deserialize(DataInput in, int available) throws IOException {

		DataInputInternal in2 = (DataInputInternal) in;

		int key = Integer.MIN_VALUE;

		byte type = in.readByte();
		boolean hasAttributes = checkAttributesFlag(type);
		boolean hasExtension = checkExtensionFlag(type);
		boolean hasPool = checkPoolFlag(type);

		type = getRealType(type);

		ExternalAttribute attr = null;
		switch (type) {
			case BOOLEAN_TRUE :
				attr = new XAttributeBooleanExternalImpl(key, true, null, null);
				break;
			case BOOLEAN_FALSE :
				attr = new XAttributeBooleanExternalImpl(key, false, null, null);
				break;
			case CONTINUOUS :
				attr = new XAttributeContinuousExternalImpl(key, in2.readDouble(), null, null);
				break;
			case DISCRETE :
				attr = new XAttributeDiscreteExternalImpl(key, in2.unpackLong(), null, null);
				break;
			case LITERAL :
				if (hasPool) {
					int poolIndex = in2.unpackInt();
					String value = literalPool.getValue(poolIndex);
					assert value != null : "Could not find value of literal in pool " + poolIndex + ", content "
							+ literalPool;
					attr = new XAttributeLiteralExternalImpl(key, value, null, null);
				} else {
					attr = new XAttributeLiteralExternalImpl(key, in2.readUTF(), null, null);
				}
				break;
			case TIMESTAMP :
				long relativeTimestamp = in2.unpackLong();
				attr = new XAttributeTimestampExternalImpl(key, relativeTimestamp + TIMESTAMP_REFERENCE, null, null);
				break;
			case ID :
				XID read = XID.read(in2);
				attr = new XAttributeIDExternalImpl(key, read, null, null);
				break;
			case CONTAINER :
				attr = new XAttributeContainerExternalImpl(key, null, null, null);
				break;
			case LIST :
				int size = in2.unpackInt();
				List<String> keyOrder = new ArrayList<>(size);
				for (int i = 0; i < size; i++) {
					keyOrder.add(in2.readUTF());
				}
				attr = new XAttributeListExternalImpl(key, keyOrder, null, null, null);
				break;
			default :
				throw new XESLiteException("De-Serialization failed for attribute with key " + key);
		}

		if (hasExtension) {
			byte ext = in2.readByte();
			attr.setExtension(XExtensionManager.instance().getByIndex(ext));
		}

		if (hasAttributes) {
			long metaAttributesId = in2.unpackLong();
			attr.setExternalId(metaAttributesId);
		}
		return attr;
	}

	private int getExtensionIndex(XExtension extension) {
		if (extension == null) {
			return -1;
		} else {
			Integer index = getExtensionMap().get(extension);
			if (index != null) {
				return index.intValue();
			} else {
				int index2 = XExtensionManager.instance().getIndex(extension);
				getExtensionMap().put(extension, index2);
				return index2;
			}
		}
	}

	private static byte addMetaData(boolean hasAttributes, boolean hasExtension, boolean shouldPool, int typeInt) {
		byte type = (byte) typeInt;
		assert (type & (ATTRIBUTES_BITMASK | EXTENSIONS_BITMASK
				| POOL_BITMASK)) == 0 : "Type in XAttributeSerializer should not be negative!";
		if (hasAttributes) {
			type |= ATTRIBUTES_BITMASK;
		}
		if (hasExtension) {
			type |= EXTENSIONS_BITMASK;
		}
		if (shouldPool) {
			type |= POOL_BITMASK;
		}
		return type;
	}

	private static byte getRealType(byte type) {
		// Reset the meta data bits
		type &= ~(ATTRIBUTES_BITMASK | EXTENSIONS_BITMASK | POOL_BITMASK);
		return type;
	}

	private static boolean checkAttributesFlag(byte type) {
		// Check if attribute bit is set
		return (byte) (type & ATTRIBUTES_BITMASK) != 0;
	}

	private static boolean checkExtensionFlag(byte type) {
		// Check if extension bit is set
		return (byte) (type & EXTENSIONS_BITMASK) != 0;
	}

	private static boolean checkPoolFlag(byte type) {
		// Check if pool bit is set
		return (byte) (type & POOL_BITMASK) != 0;
	}

	private Map<XExtension, Integer> getExtensionMap() {
		return extensionMap;
	}

	private boolean shouldPool(ExternalAttribute a) {
		if (a.getOwner() instanceof XEvent) {
			// Workaround to only pool attributes of events to avoid polluting the pool with trace id's
			return keysToPool.contains(a.getInternalKey());
		} else {
			return false;
		}
	}

	public boolean isTrusted() {
		return true;
	}

}