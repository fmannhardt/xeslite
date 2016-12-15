package org.xeslite.common;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryRegistry;
import org.deckfour.xes.id.XID;
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
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XTrace;

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
	
	/**
	 * Returns the Java class of the {@link XAttribute} value.
	 * 
	 * @param attribute
	 * @return class of the attribute
	 */
	public static Class<?> getAttributeClass(XAttribute attribute) {
		if (attribute instanceof XAttributeLiteral) {
			return String.class;
		} else if (attribute instanceof XAttributeBoolean) {
			return Boolean.class;
		} else if (attribute instanceof XAttributeContinuous) {
			return Double.class;
		} else if (attribute instanceof XAttributeDiscrete) {
			return Long.class;
		} else if (attribute instanceof XAttributeTimestamp) {
			return Date.class;
		} else if (attribute instanceof XAttributeID) {
			return XID.class;
		} else {
			throw new IllegalArgumentException("Unexpected attribute type!");
		}
	}
	
	public static Set<String> getEventAttributeKeys(Iterable<XTrace> traces) {
		Set<String> attributeKeys = new HashSet<>();
		for (XTrace t : traces) {
			for (XEvent e : t) {
				attributeKeys.addAll(e.getAttributes().keySet());
			}
		}
		return attributeKeys;
	}

	public static Map<String, Class<?>> getEventAttributeTypes(Iterable<XTrace> traces) {
		Map<String, Class<?>> attributeTypes = new HashMap<String, Class<?>>();
		for (XTrace t : traces) {
			for (XEvent e : t) {
				for (XAttribute a : e.getAttributes().values()) {
					fillAttributeType(attributeTypes, a);
				}
			}
		}
		return attributeTypes;
	}

	public static Set<String> getTraceAttributeKeys(Iterable<XTrace> traces) {
		Set<String> attributeKeys = new HashSet<>();
		for (XTrace t : traces) {
			attributeKeys.addAll(t.getAttributes().keySet());
		}
		return attributeKeys;
	}

	public static Map<String, Class<?>> getTraceAttributeTypes(Iterable<XTrace> traces) {
		Map<String, Class<?>> attributeTypes = new HashMap<String, Class<?>>();
		for (XTrace t : traces) {
			for (XAttribute a : t.getAttributes().values()) {
				fillAttributeType(attributeTypes, a);
			}
		}
		return attributeTypes;
	}

	private static void fillAttributeType(Map<String, Class<?>> attributeTypes, XAttribute attribute) {
		if (!attributeTypes.containsKey(attribute.getKey())) {
			attributeTypes.put(attribute.getKey(), getAttributeClass(attribute));
		}
	}

}
