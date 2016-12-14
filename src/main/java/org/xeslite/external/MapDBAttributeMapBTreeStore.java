package org.xeslite.external;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.deckfour.xes.model.XAttributeMap;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * Implements the {@link XAttributeMap} interface using a {@link NavigableMap}
 * from MapDB as backend. This implementation tries to forward most operations
 * directly to the backing map to avoid unnecessary copies. An exception is the
 * method {@link #entrySet()} that returns an immutable copy of the attributes,
 * so better use {@link #values()} and use the method
 * {@link XAttributeExternalImpl#getKey()} to get the key of the attribute.
 * 
 * @author F. Mannhardt
 * 
 */
class MapDBAttributeMapBTreeStore extends ExternalAttributeMap<MapDBStore> {

	private NavigableMap<Long, ExternalAttribute> subMap;

	/**
	 * Create a new {@link MapDBAttributeMapBTreeStore} for the
	 * {@link ExternalAttributable} in the {@link ExternalStore}. This class
	 * provides a view on the data stored, so no data is copied upon creating.
	 * 
	 * @param owner
	 * @param store
	 */
	public MapDBAttributeMapBTreeStore(ExternalAttributable owner, MapDBStore store) {
		super(store, owner);
	}

	final NavigableMap<Long, ExternalAttribute> getInternalFullMap() {
		return getStore().getMapStorage();
	}

	final NavigableMap<Long, ExternalAttribute> getInternalSubMap() {
		if (subMap == null) {
			subMap = getInternalFullMap().subMap(getStore().lowestCompositeKey(getOwner()), true,
					getStore().highestCompositeKey(getOwner()), true);
		}
		return subMap;
	}

	@Override
	public void clear() {
		getInternalSubMap().clear();
	}

	@Override
	public int size() {
		return getInternalSubMap().size();
	}

	@Override
	protected ExternalAttribute doPut(Integer keyIndex, ExternalAttribute a) {
		Long compositeKey = MapDBStore.getCompositeKey(getOwner(), keyIndex);
		return getInternalFullMap().put(compositeKey, a);
	}

	@Override
	protected ExternalAttribute doGet(Integer keyIndex) {
		Long compositeKey = MapDBStore.getCompositeKey(getOwner(), keyIndex);
		return getInternalFullMap().get(compositeKey);
	}

	@Override
	protected ExternalAttribute doRemove(Integer keyIndex) {
		Long compositeKey = MapDBStore.getCompositeKey(getOwner(), keyIndex);
		return getInternalFullMap().remove(compositeKey);
	}

	@Override
	protected Iterator<String> iterateKeys() {
		NavigableSet<Long> keySet = getInternalSubMap().navigableKeySet();
		return Iterators.transform(keySet.iterator(), new Function<Long, String>() {

			@Override
			public String apply(Long id) {
				int keyId = (int) (id.longValue() % getStore().getAttributeKeyPool().getCapacity());
				return getStore().getAttributeKeyPool().getValue(keyId);
			}
		});
	}

	@Override
	protected Iterator<ExternalAttribute> iterateValues() {
		return Iterators.transform(getInternalSubMap().entrySet().iterator(),
				new Function<Entry<Long, ExternalAttribute>, ExternalAttribute>() {

					public ExternalAttribute apply(Map.Entry<Long, ExternalAttribute> entry) {
						Long compositeKey = entry.getKey();
						ExternalAttribute attribute = entry.getValue();
						int attributeKey = MapDBStore.getInternalAttributeKey(getOwner(), compositeKey);
						// needs to be decorated here to avoid extra transform
						return XAttributeExternalImpl.decorate(attribute, attributeKey, getStore(), getOwner());
					}
				});
	}

}