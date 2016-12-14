package org.xeslite.external;

import java.util.Date;
import java.util.Objects;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeTimestamp;
import org.deckfour.xes.model.impl.XAttributeTimestampImpl;

import com.google.common.primitives.Longs;

/**
 * @author F. Mannhardt
 * 
 */
class XAttributeTimestampExternalImpl extends XAttributeExternalImpl implements XAttributeTimestamp {

	private static final long serialVersionUID = 1L;

	private long value;

	public XAttributeTimestampExternalImpl(int key, Date value, ExternalStore store, ExternalAttributable owner) {
		this(key, value, null, store, owner);
	}

	public XAttributeTimestampExternalImpl(int key, Date value, XExtension extension, ExternalStore store, ExternalAttributable owner) {
		this(key, value.getTime(), extension, store, owner);
	}

	public XAttributeTimestampExternalImpl(int key, long millis, ExternalStore store, ExternalAttributable owner) {
		this(key, millis, null, store, owner);
	}

	public XAttributeTimestampExternalImpl(int key, long millis, XExtension extension, ExternalStore store, ExternalAttributable owner) {
		super(key, extension, store, owner);
		this.value = millis;			
	}

	@Override
	public final Date getValue() {
		return new Date(value);
	}

	@Override
	public final long getValueMillis() {
		return this.value;
	}

	@Override
	public void setValue(Date value) {
		this.value = value.getTime();
		getOwner().getAttributes().put(getKey(), this);
	}

	@Override
	public void setValueMillis(long value) {
		this.value = value;
		getOwner().getAttributes().put(getKey(), this);
	}

	@Override
	public final String toString() {
		synchronized (FORMATTER) {
			return FORMATTER.format(getValue());
		}
	}

	@Override
	public final Object clone() {
		// Creating a 'normal' attribute since the clone is not yet saved in MapDB
		XAttributeTimestamp clone = new XAttributeTimestampImpl(getKey(), value, getExtension());
		if (hasAttributes()) {
			clone.setAttributes(getAttributes());
		}
		return clone;
	}

	@Override
	public final boolean equals(Object obj) {
		if (obj==this) 
		    return true;
		if (obj instanceof XAttributeTimestamp) { // compares types
			XAttributeTimestamp other = (XAttributeTimestamp) obj;
			return super.equals(other) // compares keys
					&& value == other.getValueMillis(); // compares values
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
		if (!(other instanceof XAttributeTimestamp)) {
			throw new ClassCastException();
		}
		int result = super.compareTo(other);
		if (result != 0) {
			return result;
		}
		return Longs.compare(value, ((XAttributeTimestamp) other).getValueMillis());
	}

}
