package org.xeslite.common;

import org.deckfour.xes.id.XID;

import com.google.common.primitives.Longs;

public final class XSeqID extends XID {

	private final long id;

	public XSeqID(long id) {
		this.id = id;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof XSeqID) {
			XSeqID other = (XSeqID) obj;
			return id == other.id;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return Long.toString(id);
	}

	@Override
	public Object clone() {
		return new XSeqID(id);
	}

	@Override
	public int hashCode() {
		return Longs.hashCode(id);
	}

	@Override
	public int compareTo(XID o) {
		if (o instanceof XSeqID) {
			return Longs.compare(this.id, ((XSeqID) o).id);
		} else {
			return -1; //TODO correct?
		}
	}

}