package org.xeslite.external;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * A {@link Set} implementation that provides a view on the entries of an
 * {@link XAttributeMap}. This implementation queries the underlying MapDB
 * storage and tries to avoid copies where possible. It implements the optional
 * {@link Entry#setValue(Object)} method of the {@link Entry} interface. It does
 * not allow for {@link ExternalAttributeEntrySet#add(Entry)}.
 * 
 * @author F. Mannhardt
 *
 */
final class ExternalAttributeEntrySet extends AbstractSet<Entry<String, XAttribute>> {

	private final XAttributeMap attributeMap;
	private final ExternalStore store;
	private final ExternalAttributable owner;
	private final Iterable<ExternalAttribute> valueIterable;

	ExternalAttributeEntrySet(XAttributeMap attributeMap, ExternalAttributable owner, ExternalStore store,
			Iterable<ExternalAttribute> valueIterable) {
		this.attributeMap = attributeMap;
		this.owner = owner;
		this.store = store;
		this.valueIterable = valueIterable;
	}

	@Override
	public Iterator<Entry<String, XAttribute>> iterator() {
		return Iterators.transform(valueIterable.iterator(),
				new Function<ExternalAttribute, Entry<String, XAttribute>>() {

					public Entry<String, XAttribute> apply(ExternalAttribute attribute) {
						return new ExternalMutableAttributeEntry(attribute, attributeMap, store, owner);
					}
				});
	}

	@Override
	public int size() {
		return attributeMap.size();
	}

	// Overridden for efficiency

	@Override
	public boolean contains(Object o) {
		String attributeKey = getAttributeKey(o);
		if (attributeKey != null) {
			XAttribute storedAttribute = attributeMap.get(attributeKey);
			if (storedAttribute != null) {
				XAttribute attribute = getAttribute(o);
				return attribute.equals(storedAttribute);
			} else {
				return false;
			}
		}
		return false;
	}

	@Override
	public boolean remove(Object o) {
		String attributeKey = getAttributeKey(o);
		if (attributeKey != null) {
			return attributeMap.remove(attributeKey) != null;
		}
		return false;
	}

	@Override
	public void clear() {
		attributeMap.clear();
	}

	private static XAttribute getAttribute(Object o) {
		if (o == null || (!(o instanceof Entry<?, ?>))) {
			return null; // null is not permitted
		}
		Entry<?, ?> entry = (Entry<?, ?>) o;
		Object value = entry.getValue();
		if (value == null || (!(value instanceof XAttribute))) {
			return null;
		}
		return (XAttribute) value;
	}

	private static String getAttributeKey(Object o) {
		if (o == null || (!(o instanceof Entry<?, ?>))) {
			return null; // null is not permitted
		}
		Entry<?, ?> entry = (Entry<?, ?>) o;
		Object key = entry.getKey();
		if (key == null || (!(key instanceof String))) {
			return null;
		}
		return (String) key;
	}

}