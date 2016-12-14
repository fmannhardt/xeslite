package org.xeslite.external;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;
import org.xeslite.lite.factory.XAttributeMapLiteImpl;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

/**
 * This {@link MapDBAttributeMapBTreeStore} looks up items in the cache first if
 * the {@link ExternalAttributable} also implements the
 * {@link AttributesCacheable} interface. It wraps an existing
 * {@link ExternalAttributeMap}.
 *
 * @author F. Mannhardt
 * 
 */
final class ExternalAttributeMapCaching implements XAttributeMap {

	private static final class CacheItr implements Iterator<XAttribute> {

		private final AttributesCacheable cachable;
		private int index = -1;
		private int last = -1;

		private CacheItr(AttributesCacheable cachable) {
			this.cachable = cachable;
			this.index = findNext();
		}

		private int findNext() {
			int i = index + 1;
			while (i < cachable.getCacheSize() && cachable.getOriginalCacheValue(i) == -1) {
				i++;
			}
			return i; // not found
		}

		private int findPrev() {
			int i = index - 1;
			while (i > 0 && cachable.getOriginalCacheValue(i) == -1) {
				i--;
			}
			return i; // not found
		}

		public boolean hasNext() {
			if (index >= cachable.getCacheSize()) {
				return false;
			} else {
				return true;
			}
		}

		public XAttribute next() {
			if (index >= cachable.getCacheSize()) {
				throw new NoSuchElementException();
			}
			XAttribute a = cachable.getCacheValue(index);
			last = index;
			index = findNext();
			return a;
		}

		public void remove() {
			if (last == -1) {
				throw new IllegalStateException();
			}
			cachable.removeCacheValue(last);
			if (last < index) {
				index = findPrev();
			}
			last = -1;
		}

	}

	private final ExternalAttributable owner;
	private final ExternalAttributeMap<? extends ExternalStore> originalMap;

	public ExternalAttributeMapCaching(ExternalAttributable owner, ExternalAttributeMap<? extends ExternalStore> originalMap) {
		this.owner = owner;
		this.originalMap = originalMap;
	}

	public ExternalAttributable getOwner() {
		return owner;
	}

	public ExternalAttributeMap<? extends ExternalStore> getOriginalMap() {
		return originalMap;
	}

	@Override
	public XAttribute put(String key, XAttribute value) {
		if (getOwner() instanceof AttributesCacheable) {
			AttributesCacheable cacheable = (AttributesCacheable) getOwner();
			Integer cacheIndex = cacheable.getCacheIndex(key);
			if (cacheIndex != null) {
				return cacheable.putCacheValue(cacheIndex, value);
			} else {
				return getOriginalMap().put(key, value);
			}
		} else {
			return getOriginalMap().put(key, value);
		}
	}

	@Override
	public XAttribute get(Object key) {
		if (getOwner() instanceof AttributesCacheable && key instanceof String) {
			AttributesCacheable cacheable = (AttributesCacheable) getOwner();
			Integer cacheIndex = cacheable.getCacheIndex((String) key);
			if (cacheIndex != null) {
				return cacheable.getCacheValue(cacheIndex);
			} else {
				return getOriginalMap().get(key);
			}
		} else {
			return getOriginalMap().get(key);
		}
	}

	@Override
	public int size() {
		return getOriginalMap().size() + sizeCached();
	}

	protected int sizeCached() {
		int size = 0;
		if (getOwner() instanceof AttributesCacheable) {
			AttributesCacheable cachable = (AttributesCacheable) getOwner();
			for (int i = 0; i < cachable.getCacheSize(); i++) {
				long c = cachable.getOriginalCacheValue(i);
				if (c != -1) {
					size++;
				}
			}
		}
		return size;
	}

	@Override
	public boolean isEmpty() {
		return sizeCached() != 0 || getOriginalMap().isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		if (getOwner() instanceof AttributesCacheable && key instanceof String) {
			AttributesCacheable cacheable = (AttributesCacheable) getOwner();
			Integer cacheIndex = cacheable.getCacheIndex((String) key);
			if (cacheIndex != null) {
				if (cacheable.getOriginalCacheValue(cacheIndex) != -1) {
					return true;
				} else {
					return false;
				}
			} else {
				return getOriginalMap().containsKey(key);
			}
		} else {
			return getOriginalMap().containsKey(key);
		}
	}

	//  super.containsValue() just uses values()

	@Override
	public final XAttribute remove(Object key) {
		if (getOwner() instanceof AttributesCacheable && key instanceof String) {
			AttributesCacheable cacheable = (AttributesCacheable) getOwner();
			Integer cacheIndex = cacheable.getCacheIndex((String) key);
			if (cacheIndex != null) {
				XAttribute oldValue = cacheable.getCacheValue(cacheIndex);
				cacheable.removeCacheValue(cacheIndex);
				return oldValue;
			} else {
				return null;
			}
		} else {
			return getOriginalMap().remove(key);
		}
	}

	@Override
	public final void clear() {
		if (getOwner() instanceof AttributesCacheable) {
			AttributesCacheable cacheable = (AttributesCacheable) getOwner();
			cacheable.clearCache();
		}
		getOriginalMap().clear();
	}

	@Override
	public Set<String> keySet() {
		final Set<String> keySet = getOriginalMap().keySet();
		if (getOwner() instanceof AttributesCacheable) {
			return new AbstractSet<String>() {

				public boolean contains(Object o) {
					return containsKey(o);
				}

				public Iterator<String> iterator() {
					return Iterators.concat(keySet.iterator(), iterateCachedKeys());
				}

				public int size() {
					return ExternalAttributeMapCaching.this.size();
				}
			};
		} else {
			return keySet;
		}
	}

	private Iterator<? extends String> iterateCachedKeys() {
		return Iterators.transform(iterateCached(), new Function<XAttribute, String>() {

			public String apply(XAttribute a) {
				return a.getKey();
			}
		});
	}

	@Override
	public Collection<XAttribute> values() {
		final Collection<XAttribute> values = getOriginalMap().values();
		if (getOwner() instanceof AttributesCacheable) {
			return new AbstractCollection<XAttribute>() {

				public Iterator<XAttribute> iterator() {
					return Iterators.concat(values.iterator(), iterateCached());
				}

				public int size() {
					return ExternalAttributeMapCaching.this.size();
				}
			};
		} else {
			return values;
		}
	}

	private Iterator<? extends XAttribute> iterateCached() {
		if (getOwner() instanceof AttributesCacheable) {
			final AttributesCacheable cachable = (AttributesCacheable) getOwner();
			return new CacheItr(cachable);
		} else {
			return ImmutableSet.<XAttribute>of().iterator();
		}
	}

	@Override
	public Set<Entry<String, XAttribute>> entrySet() {
		final Set<Entry<String, XAttribute>> entrySet = getOriginalMap().entrySet();
		if (getOwner() instanceof AttributesCacheable) {
			return new AbstractSet<Entry<String, XAttribute>>() {

				public Iterator<Entry<String, XAttribute>> iterator() {
					return Iterators.concat(entrySet.iterator(), iterateCachedEntries());
				}

				public int size() {
					return ExternalAttributeMapCaching.this.size();
				}
			};
		} else {
			return entrySet;
		}
	}

	private Iterator<Entry<String, XAttribute>> iterateCachedEntries() {
		return Iterators.transform(iterateCached(), new Function<XAttribute, Entry<String, XAttribute>>() {

			public Entry<String, XAttribute> apply(XAttribute a) {
				return new ExternalMutableAttributeEntry((ExternalAttribute) a, ExternalAttributeMapCaching.this,
						getOriginalMap().getStore(), getOwner());
			}
		});
	}

	@Override
	public boolean containsValue(Object value) {
		return values().contains(value);
	}

	@Override
	public void putAll(Map<? extends String, ? extends XAttribute> m) {
		getOriginalMap().putAll(m);
	}

	@Override
	public Object clone() {
		// Return a real in-memory copy of this map
		XAttributeMap clone = new XAttributeMapLiteImpl();
		for (XAttribute attr : values()) {
			clone.put(attr.getKey(), (XAttribute) attr.clone());
		}
		return clone;
	}

}