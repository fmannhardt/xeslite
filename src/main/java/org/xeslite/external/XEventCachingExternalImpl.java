package org.xeslite.external;

import java.util.Arrays;

import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.extension.std.XLifecycleExtension;
import org.deckfour.xes.extension.std.XTimeExtension;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeBoolean;
import org.deckfour.xes.model.XAttributeDiscrete;
import org.deckfour.xes.model.XAttributeLiteral;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XAttributeTimestamp;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

/**
 * @author F. Mannhardt
 * 
 */
class XEventCachingExternalImpl extends XEventBareExternalImpl implements AttributesCacheable {

	static final ImmutableBiMap<String, Integer> CACHED_ATTRIBUTE_KEYS = ImmutableBiMap.of("concept:name", 0,
			"lifecycle:transition", 1, "time:timestamp", 2);

	static final ImmutableMap<Integer, AttributeInfo> CACHED_ATTRIBUTES_INFO = ImmutableMap
			.<Integer, AttributeInfo>builder()
			.put(0, new AttributeInfoImpl("concept:name", XAttributeLiteral.class, XConceptExtension.instance()))
			.put(1, new AttributeInfoImpl("lifecycle:transition", XAttributeLiteral.class,
					XLifecycleExtension.instance()))
			.put(2, new AttributeInfoImpl("time:timestamp", XAttributeTimestamp.class, XTimeExtension.instance()))
			.build();

	private final long[] cache;

	XEventCachingExternalImpl(ExternalStore store) {
		this(null, store, CACHED_ATTRIBUTE_KEYS.size());
	}

	XEventCachingExternalImpl(XAttributeMap attributeMap, ExternalStore store) {
		this(attributeMap, store, CACHED_ATTRIBUTE_KEYS.size());
	}

	XEventCachingExternalImpl(long id, XAttributeMap attributeMap, ExternalStore store) {
		this(id, attributeMap, store, CACHED_ATTRIBUTE_KEYS.size());
	}

	XEventCachingExternalImpl(ExternalStore store, int cacheSize) {
		this(null, store, cacheSize);
	}

	XEventCachingExternalImpl(XAttributeMap attributeMap, ExternalStore store, int cacheSize) {
		super(null, store);
		this.cache = new long[cacheSize];
		Arrays.fill(cache, -1);
		// Need to initialize our attributes ourselves as otherwise the cached fields get overwritten by our initializer  
		initAttributes(attributeMap);
	}

	XEventCachingExternalImpl(long id, XAttributeMap attributeMap, ExternalStore store, int cacheSize) {
		super(id, null, store);
		this.cache = new long[cacheSize];
		Arrays.fill(cache, -1);
		initAttributes(attributeMap);
	}

	private final void initAttributes(XAttributeMap attributeMap) {
		if (attributeMap != null) {
			XAttributeMap newAttributeMap = getAttributes();
			for (XAttribute a : attributeMap.values()) {
				newAttributeMap.put(a.getKey(), a);
			}
		}
	}

	@Override
	public final boolean hasAttributes() {
		// Use the cached attribute as indicator, if available
		for (long c : cache) {
			if (c != -1) {
				return true;
			}
		}
		return super.hasAttributes();
	}

	@Override
	public Object clone() {
		return new XEventCachingExternalImpl(getAttributes(), getStore());
	}

	public final long[] getCache() {
		return cache;
	}

	public Integer getCacheIndex(String key) {
		return CACHED_ATTRIBUTE_KEYS.get(key);
	}

	public AttributeInfo getCacheInfo(int cacheIndex) {
		return CACHED_ATTRIBUTES_INFO.get(cacheIndex);
	}

	public void setCacheValue(int cacheIndex, XAttribute value) {
		if (value instanceof XAttributeLiteral) {
			getCache()[cacheIndex] = getStore().getLiteralPool().put(((XAttributeLiteral) value).getValue());
		} else if (value instanceof XAttributeTimestamp) {
			getCache()[cacheIndex] = ((XAttributeTimestamp) value).getValue().getTime();
		} else if (value instanceof XAttributeDiscrete) {
			getCache()[cacheIndex] = ((XAttributeDiscrete) value).getValue();
		} else if (value instanceof XAttributeBoolean) {
			getCache()[cacheIndex] = ((XAttributeBoolean) value).getValue() ? 1 : 0;
		}
	}

	public final XAttribute putCacheValue(int cacheIndex, XAttribute value) {
		XAttribute oldValue = getCacheValue(cacheIndex);
		setCacheValue(cacheIndex, value);
		return oldValue;
	}

	public final XAttribute getCacheValue(int cacheIndex) {
		AttributeInfo info = getCacheInfo(cacheIndex);
		if (info == null) {
			return null;
		} else {
			long value = getOriginalCacheValue(cacheIndex);
			if (value != -1) {
				StringPool keyPool = getStore().getAttributeKeyPool();
				if (info.getType() == XAttributeLiteral.class) {
					StringPool literalPool = getStore().getLiteralPool();
					return new XAttributeLiteralExternalImpl(keyPool.getIndex(info.getKey()),
							literalPool.getValue((int) value), info.getExtension(), getStore(), this);
				} else if (info.getType() == XAttributeTimestamp.class) {
					return new XAttributeTimestampExternalImpl(keyPool.getIndex(info.getKey()), value, info.getExtension(),
							getStore(), this);
				} else if (info.getType() == XAttributeDiscrete.class) {
					return new XAttributeDiscreteExternalImpl(keyPool.getIndex(info.getKey()), value, info.getExtension(),
							getStore(), this);
				} else if (info.getType() == XAttributeBoolean.class) {
					return new XAttributeBooleanExternalImpl(keyPool.getIndex(info.getKey()), (value == 1 ? true : false),
							info.getExtension(), getStore(), this);
				}
			}
		}
		return null;
	}

	public final long getOriginalCacheValue(int cacheIndex) {
		return cache[cacheIndex];
	}

	public final int getCacheSize() {
		return cache.length;
	}

	public void removeCacheValue(int cacheIndex) {
		cache[cacheIndex] = -1;
	}

	public void clearCache() {
		Arrays.fill(cache, -1);
	}

}