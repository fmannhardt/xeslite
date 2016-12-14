package org.xeslite.external;

import java.net.URI;

import org.deckfour.xes.extension.XExtensionManager;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeBoolean;
import org.deckfour.xes.model.XAttributeDiscrete;
import org.deckfour.xes.model.XAttributeLiteral;
import org.deckfour.xes.model.XAttributeMap;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

public class XAlignmentEventExternalImpl extends XEventCachingExternalImpl {
	
	private static final URI ALIGNMENT_EXT_URI = URI.create("http://www.xes-standard.org/alignment.xesext");
	private static final URI DATA_ALIGNMENT_EXT_URI = URI.create("http://www.xes-standard.org/dataalignment.xesext");

	static final ImmutableBiMap<String, Integer> CACHED_ALIGNMENT_KEYS = ImmutableBiMap.<String, Integer>builder()
			.put("alignment:movetype", 3)
			.put("dataalignment:movetype", 4)
			.put("alignment:observable", 5)
			.put("alignment:activityid", 6)
			.put("alignment:eventclassid", 7)
			.put("alignment:logmove", 8)
			.put("alignment:modelmove", 9).build();

	static final ImmutableMap<Integer, AttributeInfo> CACHED_ALIGNMENT_INFO = ImmutableMap.<Integer, AttributeInfo>builder()
			.put(3, new AttributeInfoImpl("alignment:movetype", XAttributeDiscrete.class, XExtensionManager.instance().getByUri(ALIGNMENT_EXT_URI)))
			.put(4, new AttributeInfoImpl("dataalignment:movetype", XAttributeDiscrete.class, XExtensionManager.instance().getByUri(DATA_ALIGNMENT_EXT_URI)))
			.put(5, new AttributeInfoImpl("alignment:observable", XAttributeBoolean.class, XExtensionManager.instance().getByUri(ALIGNMENT_EXT_URI)))
			.put(6, new AttributeInfoImpl("alignment:activityid", XAttributeLiteral.class, XExtensionManager.instance().getByUri(ALIGNMENT_EXT_URI)))			
			.put(7, new AttributeInfoImpl("alignment:eventclassid", XAttributeLiteral.class, XExtensionManager.instance().getByUri(ALIGNMENT_EXT_URI)))
			.put(8, new AttributeInfoImpl("alignment:logmove", XAttributeLiteral.class, XExtensionManager.instance().getByUri(ALIGNMENT_EXT_URI)))
			.put(9, new AttributeInfoImpl("alignment:modelmove", XAttributeLiteral.class, XExtensionManager.instance().getByUri(ALIGNMENT_EXT_URI)))
			.build();

	XAlignmentEventExternalImpl(ExternalStore store) {
		super(store, CACHED_ATTRIBUTE_KEYS.size() + CACHED_ALIGNMENT_KEYS.size());
	}

	XAlignmentEventExternalImpl(long id, XAttributeMap attributeMap, ExternalStore store) {
		super(id, null, store, CACHED_ATTRIBUTE_KEYS.size() + CACHED_ALIGNMENT_KEYS.size());
		// Need to initialize our attributes ourselves as otherwise the cached fields get overwritten by our initializer  
		initAttributes(attributeMap);
	}

	XAlignmentEventExternalImpl(XAttributeMap attributeMap, ExternalStore store) {
		super(null, store, CACHED_ATTRIBUTE_KEYS.size() + CACHED_ALIGNMENT_KEYS.size());
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

	public Integer getCacheIndex(String key) {
		Integer cacheIndex = super.getCacheIndex(key);
		if (cacheIndex == null) {
			// ensure the key is known
			getStore().getAttributeKeyPool().put(key);
			return CACHED_ALIGNMENT_KEYS.get(key);
		}
		return cacheIndex;
	}

	public AttributeInfo getCacheInfo(int cacheIndex) {
		AttributeInfo cacheInfo = super.getCacheInfo(cacheIndex);
		if (cacheInfo == null) {
			return CACHED_ALIGNMENT_INFO.get(cacheIndex);
		}
		return cacheInfo;
	}

}