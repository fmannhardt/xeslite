package org.xeslite.common;

import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryRegistry;
import org.deckfour.xes.model.XAttributable;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeBoolean;
import org.deckfour.xes.model.XAttributeContainer;
import org.deckfour.xes.model.XAttributeContinuous;
import org.deckfour.xes.model.XAttributeDiscrete;
import org.deckfour.xes.model.XAttributeID;
import org.deckfour.xes.model.XAttributeList;
import org.deckfour.xes.model.XAttributeLiteral;
import org.deckfour.xes.model.XAttributeTimestamp;

public final class XUtils {

	private XUtils() {
		super();
	}

	/**
	 * Returns the event name.
	 * 
	 * @param element
	 * @return the value of the "concept:name" attribute or "null"
	 */
	public static String getConceptName(XAttributable element) {
		return XConceptExtension.instance().extractName(element);
	}
	
	/**
	 * Creates a deep clone of the {@link XAttribute} with the same value, but a
	 * changed key.
	 * 
	 * @param oldAttribute
	 * @param newKey
	 * @return copy of the supplied attribute
	 */
	public static XAttribute cloneAttributeWithChangedKey(XAttribute oldAttribute, String newKey) {
		return cloneAttributeWithChangedKeyWithFactory(oldAttribute, newKey,
				XFactoryRegistry.instance().currentDefault());
	}

	/**
	 * Creates a deep clone of the {@link XAttribute} with the same value, but a
	 * changed key.
	 * 
	 * @param oldAttribute
	 * @param newKey
	 * @param factory
	 * @return copy of the supplied attribute
	 */
	public static XAttribute cloneAttributeWithChangedKeyWithFactory(XAttribute oldAttribute, String newKey,
			XFactory factory) {
		if (oldAttribute instanceof XAttributeList) {
			XAttributeList newAttribute = factory.createAttributeList(newKey, oldAttribute.getExtension());
			for (XAttribute a : ((XAttributeList) oldAttribute).getCollection()) {
				newAttribute.addToCollection(a);
			}
			return newAttribute;
		} else if (oldAttribute instanceof XAttributeContainer) {
			XAttributeContainer newAttribute = factory.createAttributeContainer(newKey, oldAttribute.getExtension());
			for (XAttribute a : ((XAttributeContainer) oldAttribute).getCollection()) {
				newAttribute.addToCollection(a);
			}
			return newAttribute;
		} else if (oldAttribute instanceof XAttributeLiteral) {
			return factory.createAttributeLiteral(newKey, ((XAttributeLiteral) oldAttribute).getValue(),
					oldAttribute.getExtension());
		} else if (oldAttribute instanceof XAttributeBoolean) {
			return factory.createAttributeBoolean(newKey, ((XAttributeBoolean) oldAttribute).getValue(),
					oldAttribute.getExtension());
		} else if (oldAttribute instanceof XAttributeContinuous) {
			return factory.createAttributeContinuous(newKey, ((XAttributeContinuous) oldAttribute).getValue(),
					oldAttribute.getExtension());
		} else if (oldAttribute instanceof XAttributeDiscrete) {
			return factory.createAttributeDiscrete(newKey, ((XAttributeDiscrete) oldAttribute).getValue(),
					oldAttribute.getExtension());
		} else if (oldAttribute instanceof XAttributeTimestamp) {
			return factory.createAttributeTimestamp(newKey, ((XAttributeTimestamp) oldAttribute).getValue(),
					oldAttribute.getExtension());
		} else if (oldAttribute instanceof XAttributeID) {
			return factory.createAttributeID(newKey, ((XAttributeID) oldAttribute).getValue(),
					oldAttribute.getExtension());
		} else {
			throw new IllegalArgumentException("Unexpected attribute type!");
		}
	}

}
