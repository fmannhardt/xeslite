package org.xeslite.external;

import java.util.Iterator;

import org.deckfour.xes.model.XAttribute;
import org.xeslite.external.InMemoryStore.AttributeStore;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

public class InMemoryAttributeMap extends ExternalAttributeMap<InMemoryStore> {

	public InMemoryAttributeMap(ExternalAttributable owner, InMemoryStore store) {
		super(store, owner);
	}

	private AttributeStore<ExternalAttribute> getAS() {
		return getStore().getAttributeStore();
	}

	private long getId() {
		return getOwner().getExternalId();
	}

	@Override
	public int size() {
		return getAS().size(getId());
	}
	
	@Override
	public boolean isEmpty() {
		return !getAS().iterateValues(getId()).hasNext();
	}

	@Override
	public void clear() {
		getAS().clear(getId());
	}

	@Override
	protected ExternalAttribute doPut(Integer keyIndex, ExternalAttribute a) {
		return getAS().putValue(keyIndex, getId(), a);
	}

	@Override
	protected ExternalAttribute doGet(Integer keyIndex) {
		return getAS().getValue(keyIndex, getId());
	}

	@Override
	protected ExternalAttribute doRemove(Integer keyIndex) {
		return getAS().removeValue(keyIndex, getId());
	}

	private static final Function<XAttribute, String> KEY_TRANSFORMER = new Function<XAttribute, String>() {
		public String apply(XAttribute a) {
			return a.getKey();
		}
	};

	@Override
	protected Iterator<String> iterateKeys() {
		return Iterators.transform(iterateValues(), KEY_TRANSFORMER);
	}

	@Override
	protected Iterator<ExternalAttribute> iterateValues() {
		return Iterators.transform(getAS().iterateValues(getId()),
				new Function<ExternalAttribute, ExternalAttribute>() {

					public ExternalAttribute apply(ExternalAttribute entry) {
						return XAttributeExternalImpl.decorate(entry, entry.getInternalKey(), getStore(),
								getOwner());
					}
				});
	}

}
