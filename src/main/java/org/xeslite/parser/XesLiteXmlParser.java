package org.xeslite.parser;

import java.io.InputStream;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.deckfour.xes.classification.XEventAttributeClassifier;
import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.extension.XExtensionManager;
import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryRegistry;
import org.deckfour.xes.id.XID;
import org.deckfour.xes.in.XesXmlParser;
import org.deckfour.xes.model.XAttributable;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeBoolean;
import org.deckfour.xes.model.XAttributeCollection;
import org.deckfour.xes.model.XAttributeContainer;
import org.deckfour.xes.model.XAttributeContinuous;
import org.deckfour.xes.model.XAttributeDiscrete;
import org.deckfour.xes.model.XAttributeID;
import org.deckfour.xes.model.XAttributeList;
import org.deckfour.xes.model.XAttributeLiteral;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.XVisitor;
import org.deckfour.xes.util.XTokenHelper;
import org.xeslite.external.XFactoryExternalStore;

import com.google.common.collect.ImmutableList;

/**
 * Alternative {@link XesXmlParser} that can optionally handle the batch import
 * mode of the {@link XFactoryExternalStore}.
 * 
 * @author F. Mannhardt
 *
 */
public class XesLiteXmlParser extends XesXmlParser {

	public enum ParseState {
		NONE, TRACE_GLOBALS, EVENT_GLOBALS
	}

	private static final XAttribute INVALID_ATTRIBUTE = new XAttribute() {

		private static final long serialVersionUID = 1L;

		public int compareTo(XAttribute o) {
			return o == INVALID_ATTRIBUTE ? 0 : 1;
		}

		public void setAttributes(XAttributeMap arg0) {
		}

		public boolean hasAttributes() {
			return false;
		}

		public Set<XExtension> getExtensions() {
			return null;
		}

		public XAttributeMap getAttributes() {
			return null;
		}

		public String getKey() {
			return null;
		}

		public XExtension getExtension() {
			return null;
		}

		public void accept(XVisitor arg0, XAttributable arg1) {
		}

		public Object clone() {
			return INVALID_ATTRIBUTE;
		}
	};

	private final boolean isLenient;
	private XFactoryExternalStore pumpFactory;

	public XesLiteXmlParser(boolean isLenient) {
		this(XFactoryRegistry.instance().currentDefault(), isLenient);
	}

	public XesLiteXmlParser(XFactory factory, boolean isLenient) {
		super(factory);
		this.isLenient = isLenient;
	}

	@Override
	public final String author() {
		return "F. Mannhardt";
	}

	protected void addAttribute(XAttribute attribute, XAttributable attributable) {
		if (attributable instanceof XAttributeCollection) {
			((XAttributeCollection) attributable).addToCollection(attribute);
		} else {
			XAttributeMap attributeMap = attributable.getAttributes();
			attributeMap.put(attribute.getKey(), attribute);
		}
	}

	protected XLog createLog() {
		return factory.createLog();
	}

	protected XTrace createTrace() {
		return factory.createTrace();
	}

	protected XEvent createEvent() {
		return factory.createEvent();
	}

	protected XAttributeContainer createContainer(String key, XExtension extension) {
		return factory.createAttributeContainer(key, extension);
	}

	protected XAttributeList createList(String key, XExtension extension) {
		return factory.createAttributeList(key, extension);
	}

	protected XAttributeID createId(String key, String value, XExtension extension) {
		return factory.createAttributeID(key, XID.parse(value), extension);
	}

	protected XAttributeBoolean createBoolean(String key, String value, XExtension extension) {
		return factory.createAttributeBoolean(key, Boolean.parseBoolean(value), extension);
	}

	protected XAttributeContinuous createContinuous(String key, String value, XExtension extension) {
		return factory.createAttributeContinuous(key, Double.parseDouble(value), extension);
	}

	protected XAttributeDiscrete createDiscrete(String key, String value, XExtension extension) {
		return factory.createAttributeDiscrete(key, Long.parseLong(value), extension);
	}

	protected XAttribute createDate(String key, String value, XAttribute attribute, XExtension extension) {
		Date date = xsDateTimeConversion.parseXsDateTime(value);
		if (date != null) {
			attribute = factory.createAttributeTimestamp(key, date, extension);
		}
		return attribute;
	}

	protected XAttributeLiteral createLiteral(String key, String value, XExtension extension) {
		return factory.createAttributeLiteral(key, value, extension);
	}

	private boolean isPumping(XFactory factory) {
		return (factory instanceof XFactoryExternalStore) && (((XFactoryExternalStore) factory).isPumping());
	}

	public final List<XLog> parse(InputStream is) throws Exception {

		if (isPumping(factory)) {
			pumpFactory = (XFactoryExternalStore) factory;
			factory = pumpFactory.createPumpTransferFactory();
		}

		final XMLInputFactory xmlFactory = XMLInputFactory.newInstance();
		XMLStreamReader reader = xmlFactory.createXMLStreamReader(is);

		XLog currentLog = null;
		XTrace currentTrace = null;

		final Deque<XAttributable> attributableStack = new ArrayDeque<>();
		final Deque<XAttribute> attributeStack = new ArrayDeque<>();

		final List<String[]> classifiers = new ArrayList<>();

		String currentName = null;
		ParseState state = ParseState.NONE;

		while (reader.hasNext()) {
			int xmlEvent = reader.next();

			switch (xmlEvent) {

			case XMLStreamConstants.START_ELEMENT:
				currentName = reader.getLocalName().toLowerCase(Locale.ENGLISH);

				final String key = reader.getAttributeValue(null, "key");
				final String value = reader.getAttributeValue(null, "value");

				XAttribute attribute = null;
				XExtension extension = null;
				if (key != null) {
					int colonIndex = key.indexOf(':');
					if (colonIndex > 0 && colonIndex < (key.length() - 1)) {
						String prefix = key.substring(0, colonIndex);
						extension = XExtensionManager.instance().getByPrefix(prefix);
					}
				}

				switch (currentName) {
				case "string":
					if (key != null && value != null) {
						attribute = createLiteral(key, value, extension);
					} else {
						attribute = INVALID_ATTRIBUTE;
					}
					break;
				case "date":
					if (key != null && value != null) {
						attribute = createDate(key, value, attribute, extension);
					} else {
						attribute = INVALID_ATTRIBUTE;
					}
					break;
				case "int":
					if (key != null && value != null) {
						attribute = createDiscrete(key, value, extension);
					} else {
						attribute = INVALID_ATTRIBUTE;
					}
					break;
				case "float":
					if (key != null && value != null) {
						attribute = createContinuous(key, value, extension);
					} else {
						attribute = INVALID_ATTRIBUTE;
					}
					break;
				case "boolean":
					if (key != null && value != null) {
						attribute = createBoolean(key, value, extension);
					} else {
						attribute = INVALID_ATTRIBUTE;
					}
					break;
				case "id":
					if (key != null && value != null) {
						attribute = createId(key, value, extension);
					} else {
						attribute = INVALID_ATTRIBUTE;
					}
					break;
				case "list":
					if (key != null) {
						attribute = createList(key, extension);
					} else {
						attribute = INVALID_ATTRIBUTE;
					}
					break;
				case "container":
					if (key != null) {
						attribute = createContainer(key, extension);
					} else {
						attribute = INVALID_ATTRIBUTE;
					}
					break;
				case "event":
					attributableStack.push(createEvent());
					break;
				case "trace":
					currentTrace = createTrace();
					attributableStack.push(currentTrace);
					break;
				case "log":
					currentLog = createLog();
					attributableStack.push(currentLog);
					break;
				case "extension":
					final String uriString = reader.getAttributeValue(null, "uri");
					if (uriString != null) {
						extension = XExtensionManager.instance().getByUri(URI.create(uriString));
					} else {
						String prefixString = reader.getAttributeValue(null, "prefix");
						if (prefixString != null) {
							extension = XExtensionManager.instance().getByPrefix(prefixString);
						}
					}
					if (extension != null) {
						currentLog.getExtensions().add(extension);
					}
					break;
				case "global":
					final String scope = reader.getAttributeValue(null, "scope").toLowerCase(Locale.ENGLISH);
					switch (scope) {
					case "trace":
						state = ParseState.TRACE_GLOBALS;
						break;

					case "event":
						state = ParseState.EVENT_GLOBALS;
						break;

					default:
						throw new IllegalStateException("Unexpected scope of globals: " + scope);
					}
					break;
				case "classifier":
					final String name = reader.getAttributeValue(null, "name");
					final String keys = reader.getAttributeValue(null, "keys");
					classifiers.add(new String[] { name, keys });
					break;
				}

				if (attribute != null) {
					attributeStack.push(attribute);
					attributableStack.push(attribute);
				}

				break;

			case XMLStreamConstants.END_ELEMENT:
				currentName = reader.getLocalName().toLowerCase(Locale.ENGLISH);

				switch (currentName) {
				case "string":
				case "date":
				case "int":
				case "float":
				case "boolean":
				case "id":
				case "list":
				case "container":

					// Remove ourselves from stack, no more meta-attributes for
					// us
					attributableStack.pop();
					attribute = attributeStack.pop();

					// Neither the current parent attribute nor the attribute
					// itself is invalid
					if (attribute != INVALID_ATTRIBUTE && attributableStack.peek() != INVALID_ATTRIBUTE) {

						switch (state) {

						case TRACE_GLOBALS:
							currentLog.getGlobalTraceAttributes().add(attribute);
							break;
						case EVENT_GLOBALS:
							currentLog.getGlobalEventAttributes().add(attribute);
							break;

						default:
							XAttributable attributable = attributableStack.peek();
							addAttribute(attribute, attributable);
							break;
						}
					} else {
						if (!isLenient) {
							throw new Exception("Invalid attribute in line " + reader.getLocation().getLineNumber());
						} else {
							System.err.println(
									"Warning: Invalid XES detected at line " + reader.getLocation().getLineNumber());
						}
					}

					break;

				case "event":
					XEvent event = (XEvent) attributableStack.pop();

					if (pumpFactory != null) {
						event = pumpFactory.pumpEvent(event);
					}

					currentTrace.add(event);
					break;
				case "trace":
					XTrace trace = (XTrace) attributableStack.pop();

					if (pumpFactory != null) {
						trace = pumpFactory.pumpTrace(trace);
					}

					currentLog.add(trace);
					break;
				case "global":
					state = ParseState.NONE;
					break;
				case "log":
					final XLog log = (XLog) attributableStack.pop();
					assert log == currentLog : "Wrong log!";
					fixClassifiers(currentLog, classifiers);

					if (pumpFactory != null) {
						currentLog = pumpFactory.pumpLog(currentLog);
					}
					break;
				}
				break;

			default:
				break;
			}
		}

		return ImmutableList.of(currentLog);
	}

	private final void fixClassifiers(XLog currentLog, List<String[]> classifiers) {
		// Classifier post-processing
		for (String[] classifierInput : classifiers) {
			final String name = classifierInput[0];
			final String keys = classifierInput[1];
			if (name != null && keys != null && name.length() > 0 && keys.length() > 0) {
				List<String> keysList = fixKeys(currentLog, XTokenHelper.extractTokens(keys));
				String[] keysArray = new String[keysList.size()];
				int i = 0;
				for (String currentKey : keysList) {
					keysArray[i++] = currentKey;
				}
				currentLog.getClassifiers().add(new XEventAttributeClassifier(name, keysArray));
			}

		}
	}

	/* Copied from XesXmlParser as private */

	private final List<String> fixKeys(XLog log, List<String> keys) {
		/*
		 * Try to fix the keys using the global event attributes.
		 */
		List<String> fixedKeys = fixKeys(log, keys, 0);
		return fixedKeys == null ? keys : fixedKeys;
	}

	private final List<String> fixKeys(XLog log, List<String> keys, int index) {
		if (index >= keys.size()) {
			/*
			 * keys[0,...,length-1] are matched to global event attributes.
			 */
			return keys;
		} else {
			/*
			 * keys[0,...,index-1] are matched to global event attributes. Try
			 * to match keys[index].
			 */
			if (findGlobalEventAttribute(log, keys.get(index))) {
				/*
				 * keys[index] matches a global event attribute. Try if
				 * keys[index+1,..,length-1] match with global event attributes.
				 */
				List<String> fixedKeys = fixKeys(log, keys, index + 1);
				if (fixedKeys != null) {
					/*
					 * Yes they do. Return the match.
					 */
					return fixedKeys;
				}
				/*
				 * No, they do not. Fall thru to match keys[index]+" "
				 * +keys[index+1] to a global event attribute,
				 */
			}
			/*
			 * No such global event attribute, or no match when key[index] is
			 * matched to a global event attribute. Try merging key[index] with
			 * key[index+1].
			 */
			if (index + 1 == keys.size()) {
				/*
				 * No keys[index+1]. We cannot match keys[length-1]. Fail.
				 */
				return null;
			}
			/*
			 * Copy all matched keys.
			 */
			List<String> newKeys = new ArrayList<>(keys.size() - 1);
			for (int i = 0; i < index; i++) {
				newKeys.add(keys.get(i));
			}
			/*
			 * Merge keys[index] with keys[index+1].
			 */
			newKeys.add(keys.get(index) + " " + keys.get(index + 1));
			/*
			 * Copy all keys still left to match.
			 */
			for (int i = index + 2; i < keys.size(); i++) {
				newKeys.add(keys.get(i));
			}
			/*
			 * Check match with merged key.
			 */
			return fixKeys(log, newKeys, index);
		}
	}

	private final boolean findGlobalEventAttribute(XLog log, String key) {
		for (XAttribute attribute : log.getGlobalEventAttributes()) {
			if (attribute.getKey().equals(key)) {
				return true;
			}
		}
		/*
		 * Did not find attribute with given key.
		 */
		return false;
	}

}
