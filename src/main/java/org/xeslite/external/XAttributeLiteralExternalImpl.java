package org.xeslite.external;

import java.util.Objects;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeLiteral;
import org.deckfour.xes.model.impl.XAttributeLiteralImpl;

/**
 * @author F. Mannhardt
 * 
 */
class XAttributeLiteralExternalImpl extends XAttributeExternalImpl implements XAttributeLiteral {

	private static final long serialVersionUID = 1L;
	
	private String value;
	
	public XAttributeLiteralExternalImpl(int key, String value, ExternalStore store, ExternalAttributable owner) {
		this(key, value, null, store, owner);
	}

	public XAttributeLiteralExternalImpl(int key, String value, XExtension extension, ExternalStore store, ExternalAttributable owner) {
		super(key, extension, store, owner);
		this.value = value;
	}

	@Override
	public final String getValue() {
		return this.value;
	}

	@Override
	public void setValue(String value) {
		this.value = value;
		getOwner().getAttributes().put(getKey(), this);
	}

	@Override
	public String toString() {
		return this.value;
	}

	@Override
	public final Object clone() {
		// Creating a 'normal' attribute since the clone is not yet saved in MapDB
		XAttributeLiteral clone = new XAttributeLiteralImpl(getKey(), value, getExtension());
		if (hasAttributes()) {
			clone.setAttributes(getAttributes());
		}
		return clone;
	}

	@Override
	public final boolean equals(Object obj) {
		if (obj==this) 
		    return true;
		if (obj instanceof XAttributeLiteral) { // compares types
			XAttributeLiteral other = (XAttributeLiteral) obj;
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
		if (!(other instanceof XAttributeLiteral)) {
			throw new ClassCastException();
		}
		int result = super.compareTo(other);
		if (result != 0) {
			return result;
		}
		return value.compareTo(((XAttributeLiteral) other).getValue());
	}

}
