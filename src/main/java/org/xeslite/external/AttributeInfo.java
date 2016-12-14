package org.xeslite.external;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.model.XAttribute;

interface AttributeInfo {
	String getKey();
	Class<? extends XAttribute> getType();
	XExtension getExtension();	
}