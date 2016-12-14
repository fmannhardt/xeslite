package org.xeslite.external;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.model.XAttribute;

public interface ExternalAttribute extends XAttribute {
	
	void setStore(ExternalStore store);
	
	ExternalStore getStore();
	
	void setOwner(ExternalAttributable owner);
	
	ExternalAttributable getOwner();
	
	void setInternalKey(int key);
	
	int getInternalKey();
	
	void setExternalId(long id);

	void setExtension(XExtension extension);

}
