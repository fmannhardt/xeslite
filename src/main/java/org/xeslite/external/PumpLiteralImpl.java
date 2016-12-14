package org.xeslite.external;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.impl.XAttributeMapImpl;

final class PumpLiteralImpl extends XAttributeLiteralExternalImpl {
	
	private static final long serialVersionUID = 1L;
	
	private XAttributeMap attributeMap;

	PumpLiteralImpl(int key, String value, XExtension extension, ExternalStore store, ExternalAttributable owner) {
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