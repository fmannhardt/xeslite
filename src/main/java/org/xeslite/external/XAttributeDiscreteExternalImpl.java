package org.xeslite.external;

import java.util.Objects;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeDiscrete;
import org.deckfour.xes.model.impl.XAttributeDiscreteImpl;

import com.google.common.primitives.Longs;

/**
 * @author F. Mannhardt
 * 
 */
class XAttributeDiscreteExternalImpl extends XAttributeExternalImpl implements XAttributeDiscrete {

	private static final long serialVersionUID = 1L;

	private long value;

	public XAttributeDiscreteExternalImpl(int key, long value, ExternalStore store, ExternalAttributable owner) {
		this(key, value, null, store, owner);
	}

	public XAttributeDiscreteExternalImpl(int key, long value, XExtension extension, ExternalStore store, ExternalAttributable owner) {
		super(key, extension, store, owner);
		this.value = value;
	}

	@Override
	public final long getValue() {
		return this.value;
	}

	@Override
	public void setValue(long value) {
		this.value = value;
		getOwner().getAttributes().put(getKey(), this);
	}

	@Override
	public String toString() {
		return Long.toString(value);
	}

	@Override
	public final Object clone() {
		// Creating a 'normal' attribute since the clone is not yet saved in MapDB
		XAttributeDiscrete clone = new XAttributeDiscreteImpl(getKey(), value, getExtension());
		if (hasAttributes()) {
			clone.setAttributes(getAttributes());
		}
		return clone;
	}

	@Override
	public final boolean equals(Object obj) {
		if (obj==this) 
		    return true;
		if (obj instanceof XAttributeDiscrete) { // compares types
			XAttributeDiscrete other = (XAttributeDiscrete) obj;
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
		if (!(other instanceof XAttributeDiscrete)) {
			throw new ClassCastException();
		}
		int result = super.compareTo(other);
		if (result != 0) {
			return result;
		}
		return Longs.compare(value, ((XAttributeDiscrete) other).getValue());
	}

}
