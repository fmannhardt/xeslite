package org.xeslite.external;

import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeMap;

/**
 * @author F. Mannhardt
 * 
 */
final class XEventObservableCachingExternalImpl extends XEventCachingExternalImpl {

	interface CachedEventObserver {

		void setName(int name);

		void setTransition(int transition);

		void setTime(long time);

	}

	private CachedEventObserver observer;

	XEventObservableCachingExternalImpl(long id, XAttributeMap attributeMap, ExternalStore store) {
		super(id, attributeMap, store);
	}

	public void setCacheValue(int cacheIndex, XAttribute value) {
		super.setCacheValue(cacheIndex, value);
		if (observer != null) {
			// inform observer
			AttributeInfo info = getCacheInfo(cacheIndex);
			switch (info.getKey()) {
				case "concept:name" :
					observer.setName((int) getOriginalCacheValue(cacheIndex));
					break;
				case "lifecycle:transition" :
					observer.setTransition((int) getOriginalCacheValue(cacheIndex));
					break;
				case "time:timestamp" :
					observer.setTime(getOriginalCacheValue(cacheIndex));
					break;
				default :
					break;
			}
		}
	}

	//TODO get rid of those and support the generic caching

	public void setName(int name) {
		super.getCache()[super.getCacheIndex("concept:name")] = name;
		if (observer != null) {
			observer.setName(name);
		}
	}

	public void setTransition(int transition) {
		super.getCache()[super.getCacheIndex("lifecycle:transition")] = transition;
		if (observer != null) {
			observer.setTransition(transition);
		}
	}

	public void setTime(long time) {
		super.getCache()[super.getCacheIndex("time:timestamp")] = time;
		if (observer != null) {
			observer.setTime(time);
		}
	}

	public CachedEventObserver getObserver() {
		return observer;
	}

	public void setObserver(CachedEventObserver observer) {
		this.observer = observer;
	}

}
