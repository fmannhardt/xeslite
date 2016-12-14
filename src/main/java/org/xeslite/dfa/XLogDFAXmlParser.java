package org.xeslite.dfa;

import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Date;
import java.util.Deque;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.classification.XEventNameClassifier;
import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.extension.XExtensionManager;
import org.deckfour.xes.extension.std.XConceptExtension;
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
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.XVisitor;
import org.deckfour.xes.util.XsDateTimeConversion;
import org.deckfour.xes.util.XsDateTimeConversionJava7;
import org.xeslite.common.XUtils;
import org.xeslite.lite.factory.XFactoryLiteImpl;

public final class XLogDFAXmlParser {

	private static final XAttribute IGNORED_ATTRIBUTE = new XAttribute() {

		private static final long serialVersionUID = 1L;

		public int compareTo(XAttribute o) {
			return o == IGNORED_ATTRIBUTE ? 0 : 1;
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
			return IGNORED_ATTRIBUTE;
		}
	};

	public enum ParseState {
		NONE, TRACE_GLOBALS, EVENT_GLOBALS
	}

	protected XLog createLog() {
		return tempFactory.createLog();
	}

	protected XTrace createTrace() {
		return tempFactory.createTrace();
	}

	protected XEvent createEvent() {
		return tempFactory.createEvent();
	}

	protected XAttributeContainer createContainer(String key, XExtension extension) {
		return tempFactory.createAttributeContainer(key, extension);
	}

	protected XAttributeList createList(String key, XExtension extension) {
		return tempFactory.createAttributeList(key, extension);
	}

	protected XAttributeID createId(String key, String value, XExtension extension) {
		return tempFactory.createAttributeID(key, XID.parse(value), extension);
	}

	protected XAttributeBoolean createBoolean(String key, String value, XExtension extension) {
		return tempFactory.createAttributeBoolean(key, Boolean.parseBoolean(value), extension);
	}

	protected XAttributeContinuous createContinuous(String key, String value, XExtension extension) {
		return tempFactory.createAttributeContinuous(key, Double.parseDouble(value), extension);
	}

	protected XAttributeDiscrete createDiscrete(String key, String value, XExtension extension) {
		return tempFactory.createAttributeDiscrete(key, Long.parseLong(value), extension);
	}

	protected XAttribute createDate(String key, String value, XAttribute attribute, XExtension extension) {
		Date date = xsDateTimeConversion.parseXsDateTime(value);
		if (date != null) {
			attribute = tempFactory.createAttributeTimestamp(key, date, extension);
		}
		return attribute;
	}

	protected XAttributeLiteral createLiteral(String key, String value, XExtension extension) {
		return tempFactory.createAttributeLiteral(key, value, extension);
	}

	protected void addAttribute(XAttribute attribute, XAttributable attributable) {
		XAttributeMap attributeMap = attributable.getAttributes();
		attributeMap.put(attribute.getKey(), attribute);
	}

	private final XsDateTimeConversion xsDateTimeConversion = new XsDateTimeConversionJava7();
	private final XFactoryLiteImpl tempFactory = new XFactoryLiteImpl(false);
	private final XLogDFABuilder builder = new XLogDFABuilder();

	public XLogDFAXmlParser() {
		super();
	}

	public XLog parse(InputStream is) throws Exception {
		return parse(is, new XEventNameClassifier());
	}

	public XLog parse(InputStream is, XEventClassifier classifier) throws Exception {

		final XMLInputFactory xmlFactory = XMLInputFactory.newInstance();
		XMLStreamReader reader = xmlFactory.createXMLStreamReader(is);

		XLog currentLog = null;
		XTrace currentTrace = null;

		final Set<String> classifierAttributes = new HashSet<>(Arrays.asList(classifier.getDefiningAttributeKeys()));

		final Deque<XAttributable> attributableStack = new ArrayDeque<>();
		final Deque<XAttribute> attributeStack = new ArrayDeque<>();

		String currentName = null;
		ParseState state = ParseState.NONE;

		while (reader.hasNext()) {
			int xmlEvent = reader.next();

			switch (xmlEvent) {

				case XMLStreamConstants.START_ELEMENT :
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
						case "string" :
							if (key != null && value != null
									&& considerAttribute(attributableStack, classifierAttributes, key)) {
								attribute = createLiteral(key, value, extension);
							} else {
								attribute = IGNORED_ATTRIBUTE;
							}
							break;
						case "date" :
							if (key != null && value != null
									&& considerAttribute(attributableStack, classifierAttributes, key)) {
								attribute = createDate(key, value, attribute, extension);
							} else {
								attribute = IGNORED_ATTRIBUTE;
							}
							break;
						case "int" :
							if (key != null && value != null
									&& considerAttribute(attributableStack, classifierAttributes, key)) {
								attribute = createDiscrete(key, value, extension);
							} else {
								attribute = IGNORED_ATTRIBUTE;
							}
							break;
						case "float" :
							if (key != null && value != null
									&& considerAttribute(attributableStack, classifierAttributes, key)) {
								attribute = createContinuous(key, value, extension);
							} else {
								attribute = IGNORED_ATTRIBUTE;
							}
							break;
						case "boolean" :
							if (key != null && value != null
									&& considerAttribute(attributableStack, classifierAttributes, key)) {
								attribute = createBoolean(key, value, extension);
							} else {
								attribute = IGNORED_ATTRIBUTE;
							}
							break;
						case "id" :
						case "list" :
						case "container" :
							attribute = null;
							break;
						case "event" :
							attributableStack.push(createEvent());
							break;
						case "trace" :
							currentTrace = createTrace();
							attributableStack.push(currentTrace);
							break;
						case "log" :
							currentLog = createLog();
							attributableStack.push(currentLog);
							break;
						case "extension" :
							break;
						case "global" :
							final String scope = reader.getAttributeValue(null, "scope").toLowerCase(Locale.ENGLISH);
							switch (scope) {
								case "trace" :
									state = ParseState.TRACE_GLOBALS;
									break;

								case "event" :
									state = ParseState.EVENT_GLOBALS;
									break;

								default :
									throw new IllegalStateException("Unexpected scope of globals: " + scope);
							}
							break;
						case "classifier" :
							break;
					}

					if (attribute != null) {
						attributeStack.push(attribute);
						attributableStack.push(attribute);
					}

					break;

				case XMLStreamConstants.END_ELEMENT :
					currentName = reader.getLocalName().toLowerCase(Locale.ENGLISH);

					switch (currentName) {
						case "string" :
						case "date" :
						case "int" :
						case "float" :
						case "boolean" :
						case "id" :
						case "list" :
						case "container" :

							// Remove ourselves from stack, no more meta-attributes for us
							attributableStack.pop();
							attribute = attributeStack.pop();

							// Neither the current parent attribute nor the attribute itself is invalid
							if (attribute != IGNORED_ATTRIBUTE && attributableStack.peek() != IGNORED_ATTRIBUTE) {

								switch (state) {

									case TRACE_GLOBALS :
									case EVENT_GLOBALS :
										break;

									default :
										XAttributable attributable = attributableStack.peek();
										addAttribute(attribute, attributable);
										break;
								}
							}

							break;

						case "event" :
							XEvent event = (XEvent) attributableStack.pop();
							currentTrace.add(event);
							break;
						case "trace" :
							XTrace trace = (XTrace) attributableStack.pop();
							builder.addTrace(trace);
							break;
						case "global" :
							state = ParseState.NONE;
							break;
						case "log" :
							currentLog = (XLog) attributableStack.pop(); // ignore log
							break;
					}
					break;

				default :
					break;
			}
		}

		XLogDFA dfaLog = builder.build();
		dfaLog.setName(XUtils.getConceptName(currentLog));
		return dfaLog;
	}

	private boolean considerAttribute(Deque<XAttributable> attributableStack, final Set<String> classifierAttributes,
			final String key) {
		return (attributableStack.peekFirst() instanceof XLog && key.equals(XConceptExtension.KEY_NAME))
				|| classifierAttributes.contains(key);
	}

}