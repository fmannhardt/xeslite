package org.xeslite.external;

import java.util.Map;
import java.util.Map.Entry;

import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;
import org.xeslite.common.XESLiteException;

final class ExternalMutableAttributeEntry implements Map.Entry<String, XAttribute> {

	private final XAttributeMap attributeMap;
	private final ExternalStore store;
	private final ExternalAttributable owner;

	private ExternalAttribute currentValue;

	public ExternalMutableAttributeEntry(ExternalAttribute attribute, XAttributeMap attributeMap, ExternalStore store,
			ExternalAttributable owner) {
		this.currentValue = attribute;
		this.attributeMap = attributeMap;
		this.store = store;
		this.owner = owner;
	}

	public String getKey() {
		return currentValue.getKey();
	}

	public XAttribute getValue() {
		return currentValue;
	}

	public XAttribute setValue(XAttribute value) {
		ExternalAttribute newAttribute = XAttributeExternalImpl.convert(store, owner, value);
		// Sanity check for invalid usage of this MutableEntry  
		if (currentValue.getInternalKey() != newAttribute.getInternalKey()) {
			throw new XESLiteException(
					"Cannot change the 'key' of an attribute with the 'setValue' method! Trying to change "
							+ currentValue + " into " + newAttribute);
		}
		currentValue = newAttribute;
		return attributeMap.put(value.getKey(), newAttribute);
	}

	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof Map.Entry)) {
			return false;
		}
		@SuppressWarnings("rawtypes")
		Map.Entry other = (Entry) obj;
		return getKey() == null ? other.getKey() == null
				: getKey().equals(other.getKey()) && getValue() == null ? other.getValue() == null
						: getValue().equals(other.getValue());
	}

	public int hashCode() {
		return (getKey() == null ? 0 : getKey().hashCode()) ^ (getValue() == null ? 0 : getValue().hashCode());
	}
}