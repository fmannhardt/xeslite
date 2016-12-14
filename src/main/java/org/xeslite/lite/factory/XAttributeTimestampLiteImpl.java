package org.xeslite.lite.factory;

import java.util.Date;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeTimestamp;
import org.deckfour.xes.model.impl.XAttributeImpl;

import com.google.common.primitives.Longs;

final class XAttributeTimestampLiteImpl extends XAttributeImpl implements XAttributeTimestamp {

	private static final long serialVersionUID = -5883868464604814930L;
	
	private long value;

	public XAttributeTimestampLiteImpl(String key, Date value) {
		this(key, value, null);
	}

	public XAttributeTimestampLiteImpl(String key, Date value, XExtension extension) {
		this(key, value.getTime(), extension);
	}

	public XAttributeTimestampLiteImpl(String key, long millis) {
		this(key, millis, null);
	}

	public XAttributeTimestampLiteImpl(String key, long millis, XExtension extension) {
		super(key, extension);
		this.value = millis;
	}

	@Override
	public Date getValue() {
		return new Date(value);
	}

	@Override
	public long getValueMillis() {
		return value;
	}

	@Override
	public void setValue(Date value) {
		if (value == null) {
			throw new NullPointerException(
					"No null value allowed in timestamp attribute!");
		}
		this.value = value.getTime();
	}

	@Override
	public void setValueMillis(long value) {
		this.value = value;
	}

	@Override
	public String toString() {
		//TODO avoid synchronized here
		synchronized (FORMATTER) {
			return FORMATTER.format(new Date(this.value));
		}
	}

	@Override
	public Object clone() {
		XAttributeTimestampLiteImpl clone = (XAttributeTimestampLiteImpl) super.clone();
		clone.value = value;
		return clone;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof XAttributeTimestamp) { // compares types
			XAttributeTimestamp other = (XAttributeTimestamp) obj;
			return super.equals(other) // compares keys
					&& value == other.getValueMillis(); // compares values
		} else {
			return false;
		}
	}

	@Override
	public int compareTo(XAttribute other) {
		if (!(other instanceof XAttributeTimestamp)) {
			throw new ClassCastException();
		}
		int result = super.compareTo(other);
		if (result != 0) {
			return result;
		}
		return Longs.compare(value, ((XAttributeTimestamp)other).getValueMillis());
	}

}
