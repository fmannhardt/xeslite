package org.xeslite.external;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;

/**
 * This is, in fact, a list of attributes disguised as a XAttributeMap.
 * Naturally, it does not support most of the methods. It is only to be used
 * during batch import.
 * 
 * @author F. Mannhardt
 *
 */
final class PumpAttributeMapImpl implements XAttributeMap {

	private List<XAttribute> values = new ArrayList<>();

	public List<XAttribute> values() {
		return values;
	}

	public int size() {
		return values.size();
	}

	public void putAll(Map<? extends String, ? extends XAttribute> m) {
		for (XAttribute a : m.values()) {
			put(null, a);
		}
	}

	public XAttribute put(String key, XAttribute value) {
		values.add(value);
		return null;
	}

	public Set<String> keySet() {
		throw new UnsupportedOperationException();
	}

	public boolean isEmpty() {
		return values.isEmpty();
	}

	public XAttribute get(Object key) {
		throw new UnsupportedOperationException();
	}

	public Set<Entry<String, XAttribute>> entrySet() {
		throw new UnsupportedOperationException();
	}

	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	public boolean containsKey(Object key) {
		throw new UnsupportedOperationException();
	}

	public void clear() {
		values.clear();
	}

	public Object clone() {
		throw new UnsupportedOperationException();
	}

	public XAttribute remove(Object key) {
		throw new UnsupportedOperationException();
	}

}