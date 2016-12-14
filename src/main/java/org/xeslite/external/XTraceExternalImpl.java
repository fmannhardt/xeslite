package org.xeslite.external;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.extension.std.XTimeExtension;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XAttributeTimestamp;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.XVisitor;
import org.deckfour.xes.util.XAttributeUtils;

import com.google.common.collect.ForwardingList;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;

/**
 * @author F. Mannhardt
 * 
 */
final class XTraceExternalImpl extends ForwardingList<XEvent> implements XTrace, ExternalAttributable {

	private final ExternalStore store;
	private final long id;
	private List<XEvent> events;

	public XTraceExternalImpl(ExternalStore attributeStore) {
		this(null, attributeStore);
	}
	
	public XTraceExternalImpl(XAttributeMap attributeMap, ExternalStore attributeStore) {
		this(attributeStore.getIdFactory().nextId(), attributeMap, attributeStore);
	}
	
	public XTraceExternalImpl(ExternalStore attributeStore, Collection<XEvent> events) {
		this(attributeStore.getIdFactory().nextId(), null, attributeStore, events);
	}
	
	public XTraceExternalImpl(long id, XAttributeMap attributeMap, ExternalStore attributeStore) {
		this(id, attributeMap, attributeStore, ImmutableList.<XEvent>of());
	}

	public XTraceExternalImpl(long id, XAttributeMap attributeMap, ExternalStore attributeStore, Collection<XEvent> events) {
		this.store = attributeStore;
		this.id = id;
		this.events = new ArrayList<>(events);
		if (attributeMap != null) {
			XAttributeMap newAttributeMap = getAttributes();
			for (XAttribute a: attributeMap.values()) {
				newAttributeMap.put(a.getKey(), a);
			}
		}		
	}

	public long getExternalId() {
		return id;
	}

	@Override
	public XAttributeMap getAttributes() {
		return store.getAttributes(this);
	}

	@Override
	public Set<XExtension> getExtensions() {
		return XAttributeUtils.extractExtensions(getAttributes());
	}

	@Override
	public void setAttributes(XAttributeMap attributes) {
		store.setAttributes(this, attributes);
	}
	
	@Override
	public boolean hasAttributes() {
		return store.hasAttributes(this);
	}

	@Override
	public Object clone() {
		XTrace clone = new XTraceExternalImpl(getAttributes(), store);
		for (XEvent event : this) {
			clone.add((XEvent) event.clone());
		}
		return clone;
	}

	/*
	 * Taken from XTraceImpl of OpenXES, but unsynchronized!
	 * 
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.deckfour.xes.model.XTrace#insertOrdered(org.deckfour.xes.model.XEvent
	 * )
	 */
	public int insertOrdered(XEvent event) {
		if (this.size() == 0) {
			// append if list is empty
			add(event);
			return 0;
		}
		XAttribute insTsAttr = event.getAttributes().get(XTimeExtension.KEY_TIMESTAMP);
		if (insTsAttr == null) {
			// append if event has no timestamp
			add(event);
			return (size() - 1);
		}
		Date insTs = ((XAttributeTimestamp) insTsAttr).getValue();
		for (int i = (size() - 1); i >= 0; i--) {
			XAttribute refTsAttr = get(i).getAttributes().get(XTimeExtension.KEY_TIMESTAMP);
			if (refTsAttr == null) {
				// trace contains events w/o timestamps, append.
				add(event);
				return (size() - 1);
			}
			Date refTs = ((XAttributeTimestamp) refTsAttr).getValue();
			if (insTs.before(refTs) == false) {
				// insert position reached
				add(i + 1, event);
				return (i + 1);
			}
		}
		// beginning reached, insert at head
		add(0, event);
		return 0;
	}

	@Override
	public void accept(XVisitor visitor, XLog log) {
		visitor.visitTracePre(this, log);
		for (XAttribute attribute : getAttributes().values()) {
			attribute.accept(visitor, this);
		}
		for (XEvent event : this) {
			event.accept(visitor, this);
		}
		visitor.visitTracePost(this, log);
	}

	protected List<XEvent> delegate() {
		return events;
	}

	public boolean equals(Object object) {
		if (object instanceof XTraceExternalImpl) {
			return ((XTraceExternalImpl) object).getExternalId() == id;
		} else {
			return super.equals(object);	
		}		
	}

	public int hashCode() {		
		return Longs.hashCode(id);
	}

}