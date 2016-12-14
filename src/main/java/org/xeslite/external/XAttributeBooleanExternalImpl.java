package org.xeslite.external;

import java.util.Objects;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeBoolean;
import org.deckfour.xes.model.impl.XAttributeBooleanImpl;

import com.google.common.primitives.Booleans;

/**
 * @author F. Mannhardt
 * 
 */
class XAttributeBooleanExternalImpl extends XAttributeExternalImpl implements XAttributeBoolean {

	private static final long serialVersionUID = 1L;

	private boolean value;

	public XAttributeBooleanExternalImpl(int key, boolean value, ExternalStore store, ExternalAttributable owner) {
		this(key, value, null, store, owner);
	}

	public XAttributeBooleanExternalImpl(int key, boolean value, XExtension extension, ExternalStore store,
			ExternalAttributable owner) {
		super(key, extension, store, owner);
		this.value = value;
	}

	@Override
	public final boolean getValue() {
		return value;
	}

	@Override
	public void setValue(boolean value) {
		this.value = value;
		getOwner().getAttributes().put(getKey(), this);
	}

	@Override
	public String toString() {
		return this.value ? "true" : "false";
	}

	@Override
	public final Object clone() {
		// Creating a 'normal' attribute since the clone is not yet saved in MapDB
		XAttributeBoolean clone = new XAttributeBooleanImpl(getKey(), value, getExtension());
		if (hasAttributes()) {
			clone.setAttributes(getAttributes());
		}
		return clone;
	}

	@Override
	public final boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj instanceof XAttributeBoolean) { // compares types
			XAttributeBoolean other = (XAttributeBoolean) obj;
			return super.equals(other) // compares keys
					&& (value == other.getValue()); // compares values
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
		if (!(other instanceof XAttributeBoolean)) {
			throw new ClassCastException();
		}
		int result = super.compareTo(other);
		if (result != 0) {
			return result;
		}
		return Booleans.compare(value, ((XAttributeBoolean) other).getValue());
	}

}