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

import com.google.common.collect.ImmutableList;

/**
 * Compressed storage of a trace in form of event identifies
 * 
 * @author F. Mannhardt
 * 
 */
final class XTraceCompressedExternalImpl extends XAbstractCompressedList<XEvent>
		implements XTrace, ExternalAttributable {

	private final ExternalStore store;
	private final long id;

	XTraceCompressedExternalImpl(ExternalStore attributeStore) {
		this(null, attributeStore);
	}

	XTraceCompressedExternalImpl(ExternalStore attributeStore, Collection<XEvent> events) {
		this(attributeStore.getIdFactory().nextId(), null, attributeStore, events);
	}

	XTraceCompressedExternalImpl(XAttributeMap attributeMap, ExternalStore attributeStore) {
		this(attributeStore.getIdFactory().nextId(), attributeMap, attributeStore);
	}

	XTraceCompressedExternalImpl(long id, XAttributeMap attributeMap, ExternalStore attributeStore) {
		this(id, attributeMap, attributeStore, ImmutableList.<XEvent>of());
	}

	XTraceCompressedExternalImpl(long id, XAttributeMap attributeMap, ExternalStore attributeStore,
			Collection<XEvent> events) {
		super();
		this.store = attributeStore;
		this.id = id;
		if (attributeMap != null) {
			XAttributeMap newAttributeMap = getAttributes();
			for (XAttribute a : attributeMap.values()) {
				newAttributeMap.put(a.getKey(), a);
			}
		}
		addAll(events);
	}

	protected XEvent newInstance(final int index, final long id) {
		return new XEventBareExternalImpl(id, null, store);
	}

	protected long getExternalId(XEvent e) {
		assert e instanceof XEventBareExternalImpl : "Invalid event " + e + " should be of class "
				+ XEventBareExternalImpl.class.getSimpleName();
		return ((XEventBareExternalImpl) e).getExternalId();
	}

	protected int getIdShift() {
		return store.getIdFactory().getIdShift();
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
		XTrace clone = new XTraceCompressedExternalImpl(getAttributes(), store);
		List<XEvent> clonedEvents = new ArrayList<>(size());
		for (XEvent event : this) {
			clonedEvents.add((XEvent) event.clone());
		}
		clone.addAll(clonedEvents);
		return clone;
	}

	/**
	 * copied from OpenXES
	 */
	@Override
	public int insertOrdered(XEvent event) {
		// Same as in OpenXES
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
		// Same as in OpenXES
		visitor.visitTracePre(this, log);
		for (XAttribute attribute : getAttributes().values()) {
			attribute.accept(visitor, this);
		}
		for (XEvent event : this) {
			event.accept(visitor, this);
		}
		visitor.visitTracePost(this, log);
	}

}