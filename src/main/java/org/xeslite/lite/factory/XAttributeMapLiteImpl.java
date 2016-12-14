package org.xeslite.lite.factory;

import java.io.Serializable;
import java.util.Map;

import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;

import com.google.common.collect.ForwardingMap;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

public final class XAttributeMapLiteImpl extends ForwardingMap<String, XAttribute>
		implements XAttributeMap, Serializable {

	private static final long serialVersionUID = 6928328637653669033L;

	private Map<String, XAttribute> attributeMap;

	public XAttributeMapLiteImpl() {
		attributeMap = createInternalMap();
	}

	public XAttributeMapLiteImpl(int initialSize) {
		attributeMap = createInternalMap(initialSize);
	}

	private Map<String, XAttribute> createInternalMap() {
		return new Object2ObjectOpenHashMap<>();
	}

	private Map<String, XAttribute> createInternalMap(int initialSize) {
		return new Object2ObjectOpenHashMap<>(initialSize, 0.6f);
	}

	@Override
	protected Map<String, XAttribute> delegate() {
		return attributeMap;
	}

	@Override
	public Object clone() {
		XAttributeMapLiteImpl clone = new XAttributeMapLiteImpl();
		if (!isEmpty()) {
			clone.attributeMap = createInternalMap(size());
			for (XAttribute attr : values()) {
				clone.attributeMap.put(attr.getKey(), (XAttribute) attr.clone());
			}
			return clone;
		}
		return clone;
	}

}
