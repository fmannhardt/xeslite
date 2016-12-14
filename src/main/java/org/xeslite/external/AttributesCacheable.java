package org.xeslite.external;

import org.deckfour.xes.model.XAttribute;

interface AttributesCacheable {

	Integer getCacheIndex(String key);

	AttributeInfo getCacheInfo(int cacheIndex);

	XAttribute putCacheValue(int cacheIndex, XAttribute attribute);
	
	void setCacheValue(int cacheIndex, XAttribute attribute);
	
	XAttribute getCacheValue(int cacheIndex);
	
	long getOriginalCacheValue(int cacheIndex);

	int getCacheSize();

	void removeCacheValue(int last);

	void clearCache();

}
