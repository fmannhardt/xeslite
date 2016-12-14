package org.xeslite.external;

import java.util.Set;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.id.XID;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.XVisitor;
import org.deckfour.xes.util.XAttributeUtils;

import com.google.common.primitives.Longs;

/**
 * @author F. Mannhardt
 * 
 */
class XEventBareExternalImpl implements XEvent, ExternalAttributable {

	private final ExternalStore store;
	private final long externalId;

	XEventBareExternalImpl(ExternalStore store) {
		this(null, store);
	}

	XEventBareExternalImpl(XAttributeMap attributeMap, ExternalStore store) {
		this(store.getIdFactory().nextId(), attributeMap, store);
	}

	XEventBareExternalImpl(long id, XAttributeMap attributeMap, ExternalStore store) {
		this.externalId = id;
		this.store = store;
		if (attributeMap != null) {
			XAttributeMap newAttributeMap = getAttributes();
			for (XAttribute a: attributeMap.values()) {
				newAttributeMap.put(a.getKey(), a);
			}
		}
	}

	protected final ExternalStore getStore() {
		return store;
	}

	@Override
	public XAttributeMap getAttributes() {
		return store.getAttributes(this);
	}

	@Override
	public void setAttributes(XAttributeMap attributes) {
		store.setAttributes(this, attributes);
	}

	@Override
	public Set<XExtension> getExtensions() {
		return XAttributeUtils.extractExtensions(getAttributes());
	}
	
	@Override
	public boolean hasAttributes() {
		return store.hasAttributes(this);
	}

	@Override
	public Object clone() {
		return new XEventBareExternalImpl(getAttributes(),  store);
	}

	@Override
	public final boolean equals(Object o) {
		if (o == this) {
			return true;
		} else if (o instanceof XEventBareExternalImpl) {
			return ((XEventBareExternalImpl) o).getExternalId() == getExternalId();
		} else {
			return false;
		}
	}

	@Override
	public final int hashCode() {
		return Longs.hashCode(getExternalId());
	}

	public final long getExternalId() {
		return externalId;
	}

	@Override
	public XID getID() {
		return new XID(getExternalId(), getExternalId());
	}

	@Override
	public void accept(XVisitor visitor, XTrace trace) {
		visitor.visitEventPre(this, trace);
		for (XAttribute attribute : getAttributes().values()) {
			attribute.accept(visitor, this);
		}
		visitor.visitEventPost(this, trace);
	}
	
	@Override
	public String toString() {
		return super.toString();
	}

}