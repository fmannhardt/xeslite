package org.xeslite.lite.factory;

import java.net.URI;
import java.util.Date;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryRegistry;
import org.deckfour.xes.id.XID;
import org.deckfour.xes.model.XAttributeBoolean;
import org.deckfour.xes.model.XAttributeContainer;
import org.deckfour.xes.model.XAttributeContinuous;
import org.deckfour.xes.model.XAttributeDiscrete;
import org.deckfour.xes.model.XAttributeID;
import org.deckfour.xes.model.XAttributeList;
import org.deckfour.xes.model.XAttributeLiteral;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XAttributeTimestamp;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.impl.XAttributeBooleanImpl;
import org.deckfour.xes.model.impl.XAttributeContainerImpl;
import org.deckfour.xes.model.impl.XAttributeContinuousImpl;
import org.deckfour.xes.model.impl.XAttributeDiscreteImpl;
import org.deckfour.xes.model.impl.XAttributeIDImpl;
import org.deckfour.xes.model.impl.XAttributeListImpl;
import org.deckfour.xes.model.impl.XAttributeLiteralImpl;
import org.deckfour.xes.model.impl.XLogImpl;
import org.deckfour.xes.model.impl.XTraceImpl;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

/**
 * Creating lightweight XES objects with a smaller memory footprint than the
 * original naive implementation. Attributes are stored in a fastutil
 * OpenHashMap instead of the normal Java HashMap avoiding the overhead of
 * having an Entry object for each attribute.
 * <p>
 * All attribute names and some literal values are de-duplicated using an
 * {@link Interner} . We also use a simple sequential ID instead of a UUID. This
 * saves approximately 16 bytes per event (8 byte for the object header and 8
 * byte for a long instead of two longs).
 * 
 * @author F. Mannhardt
 * 
 */
public final class XFactoryLiteImpl implements XFactory {

	public static void register() {
		XFactoryRegistry.instance().register(new XFactoryLiteImpl());
	}

	private final Interner<String> interner;

	public XFactoryLiteImpl() {
		this(true);
	}

	public XFactoryLiteImpl(boolean useInterner) {
		super();
		if (useInterner) {
			interner = Interners.newStrongInterner();
		} else {
			interner = new Interner<String>() {

				public String intern(String s) {
					return s;
				}
			};
		}
	}

	private final String intern(String s) {
		return interner.intern(s);
	}

	@Override
	public String getName() {
		return "XESLite: Sequential IDs & Open Hash Map";
	}

	@Override
	public String toString() {
		return getName();
	}

	@Override
	public String getAuthor() {
		return "F. Mannhardt";
	}

	@Override
	public URI getUri() {
		return URI.create("http://www.xes-standard.org/");
	}

	@Override
	public String getVendor() {
		return "xes-standard.org";
	}

	@Override
	public String getDescription() {
		return "A XES Factory that provides objects optimized for a small memory footprint. All operations are NOT synchronized!";
	}

	@Override
	public XLog createLog() {
		return new XLogImpl(createAttributeMap());
	}

	@Override
	public XLog createLog(XAttributeMap attributes) {
		return new XLogImpl(attributes);
	}

	@Override
	public XTrace createTrace() {
		return new XTraceImpl(new XAttributeMapLiteImpl());
	}

	@Override
	public XTrace createTrace(XAttributeMap attributes) {
		return new XTraceImpl(attributes);
	}

	@Override
	public XEvent createEvent() {
		return new XEventLiteImpl();
	}

	@Override
	public XEvent createEvent(XAttributeMap attributes) {
		return new XEventLiteImpl(attributes);
	}

	@Override
	public XEvent createEvent(XID id, XAttributeMap attributes) {
		throw new UnsupportedOperationException("Cannot create an XEvent with a pre-defined ID");
	}

	@Override
	public XAttributeMap createAttributeMap() {
		return new XAttributeMapLiteImpl();
	}

	@Override
	public XAttributeBoolean createAttributeBoolean(String key, boolean value, XExtension extension) {
		return new XAttributeBooleanImpl(intern(key), value, extension);
	}

	@Override
	public XAttributeContinuous createAttributeContinuous(String key, double value, XExtension extension) {
		return new XAttributeContinuousImpl(intern(key), value, extension);
	}

	@Override
	public XAttributeDiscrete createAttributeDiscrete(String key, long value, XExtension extension) {
		return new XAttributeDiscreteImpl(intern(key), value, extension);
	}

	@Override
	public XAttributeLiteral createAttributeLiteral(String key, String value, XExtension extension) {
		return new XAttributeLiteralImpl(intern(key), value.length() < 64 ? intern(value) : value, extension);
	}

	@Override
	public XAttributeTimestamp createAttributeTimestamp(String key, Date value, XExtension extension) {
		return new XAttributeTimestampLiteImpl(intern(key), value, extension);
	}

	@Override
	public XAttributeTimestamp createAttributeTimestamp(String key, long millis, XExtension extension) {
		return new XAttributeTimestampLiteImpl(intern(key), millis, extension);
	}

	@Override
	public XAttributeID createAttributeID(String key, XID value, XExtension extension) {
		return new XAttributeIDImpl(intern(key), value, extension);
	}

	public XAttributeContainer createAttributeContainer(String key, XExtension extension) {
		return new XAttributeContainerImpl(intern(key), extension);
	}

	public XAttributeList createAttributeList(String key, XExtension extension) {
		return new XAttributeListImpl(intern(key), extension);
	}

}