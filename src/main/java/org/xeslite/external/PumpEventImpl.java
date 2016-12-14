package org.xeslite.external;

import java.util.Set;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.id.XID;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.XVisitor;

final class PumpEventImpl implements XEvent {

	private XAttributeMap attributes;

	public PumpEventImpl(XAttributeMap attributes) {
		this.attributes = attributes;
	}

	public XAttributeMap getAttributes() {
		return attributes;
	}

	public void setAttributes(XAttributeMap attributes) {
		this.attributes = attributes;
	}

	public boolean hasAttributes() {
		return !attributes.isEmpty();
	}

	public Set<XExtension> getExtensions() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object clone() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof PumpEventImpl) {
			return o == this;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return System.identityHashCode(this);
	}

	public XID getID() {
		throw new UnsupportedOperationException();
	}

	public void accept(XVisitor visitor, XTrace trace) {
		throw new UnsupportedOperationException();
	}

}