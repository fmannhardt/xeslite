package org.xeslite.external;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeList;
import org.xeslite.common.XUtils;

import com.google.common.collect.ImmutableList;

class XAttributeListExternalImpl extends XAttributeLiteralExternalImpl implements XAttributeList {

	private static final long serialVersionUID = 1L;

	private final List<String> keyOrder;

	public XAttributeListExternalImpl(int key, ExternalStore store, ExternalAttributable owner) {
		this(key, null, null, store, owner);
	}

	public XAttributeListExternalImpl(int key, XExtension extension, ExternalStore store, ExternalAttributable owner) {
		this(key, null, extension, store, owner);
	}

	XAttributeListExternalImpl(int key, List<String> keyOrder, XExtension extension, ExternalStore store, ExternalAttributable owner) {
		super(key, "", extension, store,owner);
		if (keyOrder == null) {
			this.keyOrder = new ArrayList<>();
		} else {
			this.keyOrder = keyOrder;
		}
	}

	List<String> getKeyOrder() {
		return keyOrder;
	}

	public synchronized void addToCollection(XAttribute attribute) {
		String listKey = keyOrder.size() + attribute.getKey();
		XAttribute listAttribute = XUtils.cloneAttributeWithChangedKey(attribute, listKey);
		getKeyOrder().add(listKey);
		getAttributes().put(listKey, listAttribute);
	}
	
	//TODO how to implement hashCode and equals!

	public synchronized Collection<XAttribute> getCollection() {
		Collection<XAttribute> attributeCollection = new ArrayList<>();
		ListIterator<String> iter = getKeyOrder().listIterator();
		while (iter.hasNext()) {
			int index = iter.nextIndex();
			XAttribute attribute = getAttributes().get(iter.next());
			assert attribute != null : "Inconsistent XAttributeList in MapDB!";
			int digits = index == 0 ? 1 : 1 + (int) Math.log10(index);
			attribute = XUtils.cloneAttributeWithChangedKey(attribute, attribute.getKey().substring(digits));
			attributeCollection.add(attribute);
		}
		return ImmutableList.copyOf(attributeCollection);
	}

	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		String sep = "[";
		for (XAttribute attribute : getCollection()) {
			buf.append(sep);
			sep = ",";
			buf.append(attribute.getKey());
			buf.append(":");
			buf.append(attribute.toString());
		}
		if (buf.length() == 0) {
			buf.append("[");
		}
		buf.append("]");
		return buf.toString();
	}

}