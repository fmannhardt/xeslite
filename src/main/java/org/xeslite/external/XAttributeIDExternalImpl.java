package org.xeslite.external;

import java.util.Objects;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.id.XID;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeID;
import org.deckfour.xes.model.impl.XAttributeIDImpl;

/**
 * @author F. Mannhardt
 * 
 */
class XAttributeIDExternalImpl extends XAttributeExternalImpl implements XAttributeID {

	private static final long serialVersionUID = 1L;

	private XID value;

	public XAttributeIDExternalImpl(int key, XID value, ExternalStore store, ExternalAttributable owner) {
		this(key, value, null, store, owner);
	}

	public XAttributeIDExternalImpl(int key, XID value, XExtension extension, ExternalStore store, ExternalAttributable owner) {
		super(key, extension, store, owner);
		this.value = value;
	}

	@Override
	public final XID getValue() {
		return this.value;
	}

	@Override
	public void setValue(XID value) {
		this.value = value;
		getOwner().getAttributes().put(getKey(), this);
	}

	@Override
	public String toString() {
		return this.value.toString();
	}

	@Override
	public final Object clone() {
		// Creating a 'normal' attribute since the clone is not yet saved in MapDB		
		XAttributeID clone = new XAttributeIDImpl(getKey(), value, getExtension());
		if (hasAttributes()) {
			clone.setAttributes(getAttributes());
		}
		return clone;
	}

	@Override
	public final boolean equals(Object obj) {
		if (obj==this) 
		    return true;
		if (obj instanceof XAttributeID) { // compares types
			XAttributeID other = (XAttributeID) obj;
			return super.equals(other) // compares keys
					&& value.equals(other.getValue()); // compares values
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(getInternalKey(), value);
	}

	@Override
	public final int compareTo(XAttribute other) {
		if (!(other instanceof XAttributeID)) {
			throw new ClassCastException();
		}
		int result = super.compareTo(other);
		if (result != 0) {
			return result;
		}
		return value.compareTo(((XAttributeID) other).getValue());
	}

}
