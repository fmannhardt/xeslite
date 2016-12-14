package org.xeslite.external;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.model.XAttributable;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeBoolean;
import org.deckfour.xes.model.XAttributeContainer;
import org.deckfour.xes.model.XAttributeContinuous;
import org.deckfour.xes.model.XAttributeDiscrete;
import org.deckfour.xes.model.XAttributeID;
import org.deckfour.xes.model.XAttributeList;
import org.deckfour.xes.model.XAttributeLiteral;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XAttributeTimestamp;
import org.deckfour.xes.model.XVisitor;
import org.deckfour.xes.util.XAttributeUtils;

import com.google.common.primitives.Ints;

/**
 * @author F. Mannhardt
 * 
 */
abstract class XAttributeExternalImpl implements ExternalAttributable, ExternalAttribute {

	private static final long serialVersionUID = 1L;

	protected static final String IMMUTABLE_ERROR = "XESLite implementation does not support this operation."
			+ " The XAttribute is immutable, as it may be serialized by the storage engine at any time!";

	private int key = Integer.MIN_VALUE;
	private XExtension extension;

	// Support for meta-attributes
	private ExternalStore store;
	private ExternalAttributable owner;
	private long externalId = Long.MIN_VALUE;

	protected XAttributeExternalImpl(int key, ExternalStore store, ExternalAttributable owner) {
		this(key, null, store, owner);
	}

	protected XAttributeExternalImpl(int key, XExtension extension, ExternalStore store, ExternalAttributable owner) {
		this.key = key;
		this.extension = extension;
		this.store = store;
		this.owner = owner;
	}

	public abstract Object clone();

	@Override
	public void setInternalKey(int key) {
		this.key = key;
	}

	@Override
	public int getInternalKey() {
		return key;
	}

	@Override
	public void setExternalId(long id) {
		this.externalId = id;
	}

	@Override
	public long getExternalId() {
		return externalId;
	}

	@Override
	public String getKey() {
		assert key != Integer.MIN_VALUE : "Internal key of attribute is not initialized properly!";
		String s = store.getAttributeKeyPool().getValue(key);
		assert s != null : "Key " + key + " of attribute is unknown in pool: " + store.getAttributeKeyPool().toString();
		return s;
	}

	@Override
	public XExtension getExtension() {
		return extension;
	}

	@Override
	public boolean hasAttributes() {
		return externalId != Long.MIN_VALUE ? true : false;
	}

	@Override
	public synchronized XAttributeMap getAttributes() {
		if (hasAttributes()) {
			return getStore().getAttributes(this);
		} else {
			this.externalId = store.getIdFactory().nextId();
			return getStore().getAttributes(this);
		}
	}

	@Override
	public synchronized void setAttributes(XAttributeMap attributes) {
		if (hasAttributes()) {
			store.setAttributes(this, attributes);
		} else {
			this.externalId = store.getIdFactory().nextId();
			store.setAttributes(this, attributes);
		}
	}

	@Override
	public Set<XExtension> getExtensions() {
		if (hasAttributes()) {
			Set<XExtension> metaExtensions = XAttributeUtils.extractExtensions(getAttributes());
			Set<XExtension> allExtensions = new HashSet<>(metaExtensions);
			if (getExtension() != null) {
				allExtensions.add(getExtension());
			}
			return allExtensions;
		} else {
			return Collections.emptySet();
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ExternalAttribute) {
			return ((ExternalAttribute) obj).getInternalKey() == key;
		} else if (obj instanceof XAttribute) {
			XAttribute other = (XAttribute) obj;
			return other.getKey().equals(getKey());
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Ints.hashCode(key);
	}

	@Override
	public int compareTo(XAttribute o) {
		if (o instanceof ExternalAttribute) {
			return compare(key, ((ExternalAttribute) o).getInternalKey());
		} else {
			return getKey().compareTo(o.getKey());
		}
	}

	private static int compare(int x, int y) {
		return (x < y) ? -1 : ((x == y) ? 0 : 1);
	}

	@Override
	public void accept(XVisitor visitor, XAttributable parent) {
		visitor.visitAttributePre(this, parent);
		// Does not do anything here!
		visitor.visitAttributePost(this, parent);
	}

	public void setExtension(XExtension extension) {
		this.extension = extension;
	}

	@Override
	public ExternalStore getStore() {
		return store;
	}

	@Override
	public void setStore(ExternalStore store) {
		this.store = store;
	}

	public ExternalAttributable getOwner() {
		return owner;
	}

	public void setOwner(ExternalAttributable owner) {
		this.owner = owner;
	}

	// TODO meta attributes get lost
	static ExternalAttribute convert(ExternalStore store, ExternalAttributable owner, XAttribute value) {
		if (value instanceof ExternalAttribute) {
			ExternalAttribute externalAttr = (ExternalAttribute) value;
			if (externalAttr.getStore() == store) {
				externalAttr.setOwner(owner); // make sure the owner is up to date
				// This attribute belongs to the same store, so no need to do anything				
				return externalAttr;
			} else {
				// This attribute belongs to a different store, we to create a new instance related to our store
				return convertStandardAttributes(store, owner, externalAttr, store.getAttributeKeyPool());
			}
		}
		return convertStandardAttributes(store, owner, value, store.getAttributeKeyPool());
	}

	private static ExternalAttribute convertStandardAttributes(ExternalStore store, ExternalAttributable owner,
			XAttribute value, StringPool stringPool) {
		int internalAttributeKey = stringPool.put(value.getKey());
		if (value instanceof XAttributeLiteral) {
			return new XAttributeLiteralExternalImpl(internalAttributeKey, ((XAttributeLiteral) value).getValue(),
					value.getExtension(), store, owner);
		} else if (value instanceof XAttributeTimestamp) {
			return new XAttributeTimestampExternalImpl(internalAttributeKey, ((XAttributeTimestamp) value).getValue(),
					value.getExtension(), store, owner);
		} else if (value instanceof XAttributeDiscrete) {
			return new XAttributeDiscreteExternalImpl(internalAttributeKey, ((XAttributeDiscrete) value).getValue(),
					value.getExtension(), store, owner);
		} else if (value instanceof XAttributeContinuous) {
			return new XAttributeContinuousExternalImpl(internalAttributeKey, ((XAttributeContinuous) value).getValue(),
					value.getExtension(), store, owner);
		} else if (value instanceof XAttributeBoolean) {
			return new XAttributeBooleanExternalImpl(internalAttributeKey, ((XAttributeBoolean) value).getValue(),
					value.getExtension(), store, owner);
		} else if (value instanceof XAttributeID) {
			return new XAttributeIDExternalImpl(internalAttributeKey, ((XAttributeID) value).getValue(),
					value.getExtension(), store, owner);
		} else if (value instanceof XAttributeContainer) {
			return new XAttributeContainerExternalImpl(internalAttributeKey, value.getExtension(), store, owner);
		} else if (value instanceof XAttributeList) {
			return new XAttributeListExternalImpl(internalAttributeKey, value.getExtension(), store, owner);
		} else {
			throw new IllegalArgumentException(
					"Attribute type unsupported by XESLite " + value.getClass().getSimpleName());
		}
	}

	static ExternalAttribute decorate(ExternalAttribute undecoratedAttr, int key, ExternalStore store, ExternalAttributable owner) {
		if (undecoratedAttr == null) {
			return null;
		}
		undecoratedAttr.setInternalKey(key);
		undecoratedAttr.setStore(store);
		undecoratedAttr.setOwner(owner);
		return undecoratedAttr;
	}

}