package org.xeslite.external;

import java.util.Iterator;

import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.impl.XAttributeMapImpl;
import org.xeslite.common.XESLiteException;

abstract class ExternalStoreAbstract implements ExternalStore {

	abstract protected XAttributeMap createAttributeMap(ExternalAttributable attributable);

	@Override
	public final XAttributeMap getAttributes(final ExternalAttributable attributable) {
		if (isPumping()) {
			throw new XESLiteException("Cannot be used during data pump!");
		}
		return createAttributeMap(attributable);
	}

	@Override
	public final boolean hasAttributes(final ExternalAttributable attributable) {
		if (isPumping()) {
			throw new XESLiteException("Cannot be used during data pump!");
		}
		return !createAttributeMap(attributable).isEmpty();
	}

	@Override
	public final void setAttributes(final ExternalAttributable attributable, final XAttributeMap attributes) {
		if (isPumping()) {
			throw new XESLiteException("Cannot be used during data pump!");
		}

		if (attributes.isEmpty()) {
			createAttributeMap(attributable).clear();
			return;
		}

		XAttributeMap attributeMap = createAttributeMap(attributable);
		Iterator<XAttribute> values = attributeMap.values().iterator();
		while (values.hasNext()) {
			XAttribute a = values.next();
			if (!attributes.containsKey(a.getKey())) {
				values.remove();
			}
		}
		attributeMap.putAll(attributes);
	}

	@Override
	public final XAttributeMap removeAttributes(final ExternalAttributable attributable) {
		if (isPumping()) {
			throw new XESLiteException("Cannot be used during data pump!");
		}

		XAttributeMap attributeMap = createAttributeMap(attributable);

		XAttributeMap oldMap = new XAttributeMapImpl(attributeMap.size());
		// Use values() instead of putAll() to avoid two copies (1 by entrySet() that is called by putAll(), 2 by us)
		Iterator<XAttribute> values = attributeMap.values().iterator();
		while (values.hasNext()) {
			XAttribute a = values.next();
			oldMap.put(a.getKey(), a);
		}
		attributeMap.clear();
		return oldMap;
	}

}
