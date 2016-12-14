package org.xeslite.external;

import java.util.Iterator;

import org.deckfour.xes.model.XAttribute;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

public class MapDBAttributeMapBTreeCompositeStore extends ExternalAttributeMap<MapDBStoreCompositeKey> {

	public MapDBAttributeMapBTreeCompositeStore(ExternalAttributable owner, MapDBStoreCompositeKey store) {
		super(store, owner);
	}

	@Override
	public int size() {
		return getStore().getAttributeStore().size(getOwner().getExternalId());
	}

	@Override
	public void clear() {
		getStore().getAttributeStore().clear(getOwner().getExternalId());
	}

	@Override
	protected ExternalAttribute doPut(Integer keyIndex, ExternalAttribute a) {
		return getStore().getAttributeStore().setValue(keyIndex, getOwner().getExternalId(), a);
	}

	@Override
	protected ExternalAttribute doGet(Integer keyIndex) {
		return getStore().getAttributeStore().getValue(keyIndex, getOwner().getExternalId());
	}

	@Override
	protected ExternalAttribute doRemove(Integer keyIndex) {
		return getStore().getAttributeStore().removeValue(keyIndex, getOwner().getExternalId());
	}

	@Override
	protected Iterator<String> iterateKeys() {
		return Iterators.transform(iterateValues(), new Function<XAttribute, String>() {

			public String apply(XAttribute a) {
				return a.getKey();
			}
		});
	}

	@Override
	protected Iterator<ExternalAttribute> iterateValues() {
		return Iterators.transform(getStore().getAttributeStore().iterateEntries(getOwner().getExternalId()),
				new Function<Entry<Integer, ExternalAttribute>, ExternalAttribute>() {

					public ExternalAttribute apply(Entry<Integer, ExternalAttribute> entry) {
						// needs to be decorated here to avoid extra transform
						return XAttributeExternalImpl.decorate(entry.getValue(), entry.getKey(), getStore(), getOwner());
					}
				});
	}

}
