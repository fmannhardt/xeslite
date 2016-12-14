package org.xeslite.external;

import java.util.Date;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.impl.XAttributeMapImpl;

final class PumpTimestampImpl extends XAttributeTimestampExternalImpl {
	
	private static final long serialVersionUID = 1L;
	
	private XAttributeMap attributeMap;

	PumpTimestampImpl(int key, Date value, XExtension extension, ExternalStore store, ExternalAttributable owner) {
		super(key, value, extension, store, owner);
	}

	PumpTimestampImpl(int key, long value, XExtension extension, ExternalStore store, ExternalAttributable owner) {
		super(key, value, extension, store, owner);
	}

	public boolean hasAttributes() {
		if (getExternalId() != Long.MIN_VALUE) {
			// Part of store
			return super.hasAttributes();	
		} else {
			return attributeMap != null;
		}		
	}

	public synchronized XAttributeMap getAttributes() {
		if (getExternalId() != Long.MIN_VALUE) {
			// Part of store
			return super.getAttributes();	
		} else {
			if (attributeMap == null) {
				attributeMap = new XAttributeMapImpl();
			}
			return attributeMap;
		}
	}

	public void setExternalId(long id) {
		attributeMap = null;
		super.setExternalId(id);
	}

}