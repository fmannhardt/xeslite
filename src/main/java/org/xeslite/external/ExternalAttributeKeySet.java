package org.xeslite.external;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.deckfour.xes.model.XAttributeMap;

/**
 * A {@link Set} implementation that provides a view on the keys of an
 * {@link XAttributeMap}. This implementation queries the underlying MapDB
 * storage and tries to avoid copies where possible. It does not allow for
 * {@link #add(String)}.
 * 
 * @author F. Mannhardt
 * 
 */
final class ExternalAttributeKeySet extends AbstractSet<String> {

	private final XAttributeMap attributeMap;
	private final Iterable<String> keyIterable;

	public ExternalAttributeKeySet(XAttributeMap attributeMap, Iterable<String> keyIterable) {
		this.attributeMap = attributeMap;
		this.keyIterable = keyIterable;
	}

	@Override
	public Iterator<String> iterator() {
		return keyIterable.iterator();
	}

	@Override
	public int size() {
		return attributeMap.size();
	}

	// Overridden for performance

	@Override
	public void clear() {
		attributeMap.clear();
	}

	@Override
	public boolean contains(Object o) {
		return attributeMap.containsKey(o);
	}

	@Override
	public boolean remove(Object o) {
		return attributeMap.remove(o) != null;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		boolean changed = false;
		for (Object object : c) {
			changed = remove(object);
		}
		return changed;
	}

}
