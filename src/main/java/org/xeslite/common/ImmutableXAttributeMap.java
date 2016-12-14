package org.xeslite.common;

import java.util.AbstractMap;
import java.util.Set;

import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.impl.XAttributeMapImpl;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public final class ImmutableXAttributeMap extends AbstractMap<String, XAttribute> implements XAttributeMap {

	private final XAttribute singletonAttribute;

	public ImmutableXAttributeMap(XAttribute attribute) {
		this.singletonAttribute = attribute;
	}

	public XAttributeMap clone() {
		XAttributeMapImpl clonedMap = new XAttributeMapImpl();
		clonedMap.putAll(this);
		return clonedMap;
	}

	public Set<java.util.Map.Entry<String, XAttribute>> entrySet() {
		return ImmutableSet.of(Maps.immutableEntry(singletonAttribute.getKey(), singletonAttribute));
	}

	public XAttribute get(Object key) {
		if (singletonAttribute.getKey().equals(key)) {
			return singletonAttribute;
		} else {
			return null;
		}
	}

}