package org.xeslite.external;

import java.util.AbstractCollection;
import java.util.Iterator;

import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;

class ExternalAttributeValueCollection extends AbstractCollection<XAttribute> {

	private final XAttributeMap attributeMap;
	private final Iterable<ExternalAttribute> valueIterable;

	public ExternalAttributeValueCollection(XAttributeMap attributeMap, Iterable<ExternalAttribute> valueIterable) {
		super();
		this.attributeMap = attributeMap;
		this.valueIterable = valueIterable;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Iterator<XAttribute> iterator() {
		return (Iterator) valueIterable.iterator(); // ExternalAttribute is an XAttribute
	}

	public int size() {
		return attributeMap.size();
	}

	public void clear() {
		attributeMap.clear();
	}

	// uses super.remove
	// uses super.removeAll
	// uses super.retainAll

}