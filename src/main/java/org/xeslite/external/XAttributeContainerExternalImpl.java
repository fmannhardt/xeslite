package org.xeslite.external;

import java.util.Collection;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeContainer;

class XAttributeContainerExternalImpl extends XAttributeLiteralExternalImpl implements XAttributeContainer {
	
	private static final long serialVersionUID = 1L;

	public XAttributeContainerExternalImpl(int key, XExtension extension, ExternalStore store, ExternalAttributable owner) {
		// Dummy (but unique) key, dummy value, no extension.
		super(key, "", extension, store, owner);
	}

	public void addToCollection(XAttribute attribute) {
		//TODO this is implemented different (wrong?) in OpenXES
		getAttributes().put(attribute.getKey(), attribute);
	}

	public Collection<XAttribute> getCollection() {
		return getAttributes().values();
	}
	
	//TODO how to implement hashCode and equals!
	
	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		String sep = "[";
		for (XAttribute attribute: getCollection()) {
			buf.append(sep);
			sep = ",";
			buf.append(attribute.getKey());
			buf.append(":");
			buf.append(attribute.toString());
		}
		if (buf.length() == 0) {
			buf.append("[");
		}
		buf.append("]");
		return buf.toString();
	}

}
