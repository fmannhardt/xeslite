package org.xeslite.external;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.model.XAttribute;

class AttributeInfoImpl implements AttributeInfo {

	private final String key;
	private final Class<? extends XAttribute> type;
	private final XExtension extension;	

	public AttributeInfoImpl(String key, Class<? extends XAttribute> type, XExtension extension) {
		super();
		this.key = key;
		this.type = type;
		this.extension = extension;
	}

	public Class<? extends XAttribute> getType() {
		return type;
	}

	public XExtension getExtension() {
		return extension;
	}

	public String getKey() {
		return key;
	}
}