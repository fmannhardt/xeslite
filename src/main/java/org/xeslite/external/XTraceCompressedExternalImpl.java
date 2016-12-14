package org.xeslite.external;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.extension.std.XLifecycleExtension;
import org.deckfour.xes.extension.std.XTimeExtension;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XAttributeTimestamp;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.XVisitor;
import org.deckfour.xes.util.XAttributeUtils;
import org.xeslite.external.XEventObservableCachingExternalImpl.CachedEventObserver;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

/**
 * Compressed storage of a trace in form of event identifies
 * 
 * @author F. Mannhardt
 * 
 */
final class XTraceCompressedExternalImpl extends XAbstractCompressedList<XEvent>
		implements XTrace, ExternalAttributable {

	private final class CachedEventObserverImpl implements CachedEventObserver {
		private final long id;
		private final int index;

		private CachedEventObserverImpl(long id, int index) {
			this.id = id;
			this.index = index;
		}

		public void setTransition(int transition) {
			EventData eventData = getEventData();
			ByteBuffer data = findData(index, id, eventData);
			if (data != null) {
				storeTransitionExtension(transition, data);
				setEventData(eventData);
			}
		}

		public void setTime(long time) {
			EventData eventData = getEventData();
			ByteBuffer data = findData(index, id, eventData);
			if (data != null) {
				storeTimeExtension(time, data);
				setEventData(eventData);
			}
		}

		public void setName(int name) {
			EventData eventData = getEventData();
			ByteBuffer data = findData(index, id, eventData);
			if (data != null) {
				storeConceptExtension(name, data);
				setEventData(eventData);
			}
		}

		private final ByteBuffer findData(int index, long id, EventData eventData) {
			long currentId = eventData.ids[index];
			if (currentId == id) {
				return eventData.data[index];
			} else {
				for (int i = 0; i < eventData.ids.length; i++) {
					currentId = eventData.ids[i];
					if (currentId == id) {
						return eventData.data[i];
					}
				}
			}
			return null;
		}

	}

	private static final int COMMON_ATTRIBUTES_SIZE = Longs.BYTES + Ints.BYTES + Ints.BYTES;

	private final ExternalStore store;
	private final long id;

	XTraceCompressedExternalImpl(boolean cacheCommonAttributes, ExternalStore attributeStore) {
		this(cacheCommonAttributes, null, attributeStore);
	}

	XTraceCompressedExternalImpl(boolean cacheCommonAttributes, ExternalStore attributeStore,
			Collection<XEvent> events) {
		this(attributeStore.getIdFactory().nextId(), cacheCommonAttributes, null, attributeStore, events);
	}

	XTraceCompressedExternalImpl(boolean cacheCommonAttributes, XAttributeMap attributeMap,
			ExternalStore attributeStore) {
		this(attributeStore.getIdFactory().nextId(), cacheCommonAttributes, attributeMap, attributeStore);
	}

	XTraceCompressedExternalImpl(long id, boolean cacheCommonAttributes, XAttributeMap attributeMap,
			ExternalStore attributeStore) {
		this(id, cacheCommonAttributes, attributeMap, attributeStore, ImmutableList.<XEvent> of());
	}

	XTraceCompressedExternalImpl(long id, boolean cacheCommonAttributes, XAttributeMap attributeMap,
			ExternalStore attributeStore, Collection<XEvent> events) {
		super(cacheCommonAttributes ? COMMON_ATTRIBUTES_SIZE : 0);
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

	protected XEvent newInstance(final int index, final long id, ByteBuffer data) {

		if (hasPayload()) {

			XEventObservableCachingExternalImpl event = new XEventObservableCachingExternalImpl(id, null, store);

			if (data != null) {

				assert data.hasRemaining() : "Error in XTraceCompressedExternalImpl, there is no payload for  event "
						+ event + " at index " + index;

				long timestamp;
				timestamp = data.getLong();
				if (timestamp != -1) {
					event.setTime(timestamp);
				}

				int lifecycle = data.getInt();
				if (lifecycle != -1) {
					event.setTransition(lifecycle);
				}

				int nameIndex = data.getInt();
				if (nameIndex != -1) {
					event.setName(nameIndex);
				}

				assert !data
						.hasRemaining() : "Error in XTraceCompressedExternalImpl, method newInstance did not read the entire payload for event "
								+ event + " at index " + index;

				data.flip(); // prepare for next read
			}

			event.setObserver(new CachedEventObserverImpl(id, index));

			return event;
		} else {
			return new XEventBareExternalImpl(id, null, store);
		}
	}

	protected ByteBuffer convertData(XEvent e) {

		if (hasPayload()) {

			ByteBuffer data = ByteBuffer.allocate(COMMON_ATTRIBUTES_SIZE);

			if (e instanceof AttributesCacheable) {
				// Is already cached - no need to query the store
				AttributesCacheable cached = (AttributesCacheable) e;
				storeTimeExtension(cached.getOriginalCacheValue(cached.getCacheIndex("time:timestamp")), data);
				storeTransitionExtension(
						(int) cached.getOriginalCacheValue(cached.getCacheIndex("lifecycle:transition")), data);
				storeConceptExtension((int) cached.getOriginalCacheValue(cached.getCacheIndex("concept:name")), data);
			} else {
				Date timestamp = XTimeExtension.instance().extractTimestamp(e);
				if (timestamp != null) {
					storeTimeExtension(timestamp.getTime(), data);
				} else {
					storeTimeExtension(-1, data);
				}
				String lifecycleTransition = XLifecycleExtension.instance().extractTransition(e);
				if (lifecycleTransition != null) {
					storeTransitionExtension(store.getLiteralPool().put(lifecycleTransition), data);
				} else {
					storeTransitionExtension(-1, data);
				}
				String name = XConceptExtension.instance().extractName(e);
				if (name != null) {
					storeConceptExtension(store.getLiteralPool().put(name), data);
				} else {
					storeConceptExtension(-1, data);
				}
			}

			return data;

		} else {
			return null;
		}

	}

	private static final int CONCEPT_INDEX = 12;

	private void storeConceptExtension(int name, ByteBuffer data) {
		data.putInt(CONCEPT_INDEX, name);
	}

	private static final int TRANSITION_INDEX = 8;

	private static void storeTransitionExtension(int lifecycleTransition, ByteBuffer data) {
		data.putInt(TRANSITION_INDEX, lifecycleTransition);
	}

	private static final int TIME_INDEX = 0;

	private void storeTimeExtension(long time, ByteBuffer data) {
		data.putLong(TIME_INDEX, time);
	}

	protected XEvent convertElement(XEvent e) {
		XEventBareExternalImpl newEvent;
		if (!(e instanceof XEventBareExternalImpl)) {
			newEvent = new XEventCachingExternalImpl(e.getAttributes(), store);
		} else {
			newEvent = (XEventBareExternalImpl) e;
		}
		return newEvent;
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
		XTrace clone = new XTraceCompressedExternalImpl(hasPayload(), getAttributes(), store);
		List<XEvent> clonedEvents = new ArrayList<>(size());
		for (XEvent event : this) {
			clonedEvents.add((XEvent) event.clone());
		}
		clone.addAll(clonedEvents);
		return clone;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.deckfour.xes.model.XTrace#insertOrdered(org.deckfour.xes.model.XEvent
	 * )
	 */
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