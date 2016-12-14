package org.xeslite.external;

import java.util.Objects;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeContinuous;
import org.deckfour.xes.model.impl.XAttributeContinuousImpl;

/**
 * @author F. Mannhardt
 * 
 */
class XAttributeContinuousExternalImpl extends XAttributeExternalImpl implements XAttributeContinuous {

	private static final long serialVersionUID = 1L;

	private double value;

	public XAttributeContinuousExternalImpl(int key, double value, ExternalStore store, ExternalAttributable owner) {
		this(key, value, null, store, owner);
	}

	public XAttributeContinuousExternalImpl(int key, double value, XExtension extension, ExternalStore store, ExternalAttributable owner) {
		super(key, extension, store, owner);
		this.value = value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.deckfour.xes.model.XAttributeContinuous#getValue()
	 */
	@Override
	public final double getValue() {
		return value;
	}

	@Override
	public void setValue(double value) {
		this.value = value;
		getOwner().getAttributes().put(getKey(), this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return Double.toString(this.value);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.progressmining.xes.mapdb.model.attributes.XAttributeMapDBImpl#clone()
	 */
	@Override
	public final Object clone() {
		// Creating a 'normal' attribute since the clone is not yet saved in MapDB
		XAttributeContinuous clone = new XAttributeContinuousImpl(getKey(), value, getExtension());
		if (hasAttributes()) {
			clone.setAttributes(getAttributes());
		}
		return clone;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.progressmining.xes.mapdb.model.attributes.XAttributeMapDBImpl#equals
	 * (java.lang.Object)
	 */
	@Override
	public final boolean equals(Object obj) {
		if (obj==this) 
		    return true;
		if (obj instanceof XAttributeContinuous) { // compares types
			XAttributeContinuous other = (XAttributeContinuous) obj;
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.progressmining.xes.mapdb.model.attributes.XAttributeMapDBImpl#compareTo
	 * (org.deckfour.xes.model.XAttribute)
	 */
	@Override
	public final int compareTo(XAttribute other) {
		if (!(other instanceof XAttributeContinuous)) {
			throw new ClassCastException();
		}
		int result = super.compareTo(other);
		if (result != 0) {
			return result;
		}
		return Double.compare(value, ((XAttributeContinuous) other).getValue());
	}

}
