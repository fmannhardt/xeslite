package org.xeslite.external;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;
import org.xeslite.lite.factory.XAttributeMapLiteImpl;

abstract class ExternalAttributeMap<S extends ExternalStore> extends AbstractMap<String, XAttribute>
		implements XAttributeMap {

	private final S store;
	private final ExternalAttributable owner;

	ExternalAttributeMap(S store, ExternalAttributable owner) {
		super();
		this.store = store;
		this.owner = owner;
	}

	private final Integer safeGetKey(Object key) {
		if (key instanceof String) {
			return store.getAttributeKeyPool().getIndex((String) key);
		} else {
			return null;
		}
	}

	@Override
	public XAttribute put(String key, XAttribute value) {
		if (key == null) {
			String info = (value != null ? value.toString() : "NULL");
			throw new NullPointerException(
					"This XAttributeMap implementation does not support NULL as key. Invalid attribute: " + info);
		}
		ExternalAttribute externalAttribute = XAttributeExternalImpl.convert(store, owner, value);
		assert key.equals(externalAttribute
				.getKey()) : "Trying to put attribute 'a' under a different key than return by a.getKey()";
		Integer keyIndex = store.getAttributeKeyPool().put(key);
		ExternalAttribute oldAttribute = doPut(keyIndex, externalAttribute);
		if (oldAttribute != null) {
			return XAttributeExternalImpl.decorate(oldAttribute, keyIndex, store, owner);
		} else {
			return null;
		}
	}

	@Override
	public void putAll(Map<? extends String, ? extends XAttribute> m) {
		// using values prevents creation of entries
		for (XAttribute attribute : m.values()) {
			put(attribute.getKey(), attribute);
		}
	}

	abstract protected ExternalAttribute doPut(Integer keyIndex, ExternalAttribute value);

	@Override
	public XAttribute get(Object o) {
		Integer keyIndex = safeGetKey(o);
		if (keyIndex != null) {
			ExternalAttribute attribute = doGet(keyIndex);
			return XAttributeExternalImpl.decorate(attribute, keyIndex, store, owner);
		}
		return null;
	}

	abstract protected ExternalAttribute doGet(Integer keyIndex);

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean containsKey(Object o) {
		Integer key = safeGetKey(o);
		if (key != null) {
			//TODO optimize
			return get(o) != null;
		} else {
			return false;
		}
	}

	@Override
	public XAttribute remove(Object o) {
		Integer keyIndex = safeGetKey(o);
		if (keyIndex != null) {
			ExternalAttribute removedAttribute = doRemove(keyIndex);
			if (removedAttribute != null) {
				return XAttributeExternalImpl.decorate(removedAttribute, keyIndex, store, owner);
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	abstract public void clear();

	abstract protected ExternalAttribute doRemove(Integer keyIndex);

	// super.containsValue just uses entrySet
	// super.clear() just uses entrySet

	// collection views

	@Override
	public Set<String> keySet() {
		return new ExternalAttributeKeySet(this, new Iterable<String>() {

			public Iterator<String> iterator() {
				return iterateKeys();
			}
		});
	}

	abstract protected Iterator<String> iterateKeys();

	@Override
	public Collection<XAttribute> values() {
		return new ExternalAttributeValueCollection(this, new Iterable<ExternalAttribute>() {

			public Iterator<ExternalAttribute> iterator() {
				return iterateValues();
			}
		});
	}

	/**
	 * @return an iterator over the stored {@link ExternalAttribute}, each
	 *         {@link ExternalAttribute} needs to be decorated! before being
	 *         returned.
	 */
	abstract protected Iterator<ExternalAttribute> iterateValues();

	@Override
	public Set<Entry<String, XAttribute>> entrySet() {
		return new ExternalAttributeEntrySet(this, owner, store, new Iterable<ExternalAttribute>() {

			public Iterator<ExternalAttribute> iterator() {
				return iterateValues();
			}
		});
	}

	@Override
	public Object clone() {
		// Return a real in-memory copy of this map
		XAttributeMap clone = new XAttributeMapLiteImpl();
		for (XAttribute attr : this.values()) {
			clone.put(attr.getKey(), (XAttribute) attr.clone());
		}
		return clone;
	}

	public S getStore() {
		return store;
	}

	public ExternalAttributable getOwner() {
		return owner;
	}

}