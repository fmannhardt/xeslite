package org.xeslite.external;

import java.net.URI;
import java.util.Date;

import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.factory.XFactory;
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
import org.deckfour.xes.model.impl.XLogImpl;
import org.deckfour.xes.model.impl.XTraceImpl;

final class PumpFactoryImpl implements XFactory {

	private ExternalStore targetStore;

	PumpFactoryImpl(ExternalStore targetStore) {
		super();
		this.targetStore = targetStore;
	}

	@Override
	public String getName() {
		return "XESLite: Temp Factory (Temporary only to be used for data pump)";
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
		return "";
	}

	@Override
	public String getDescription() {
		return "";
	}

	@Override
	public XLog createLog() {
		return new XLogImpl(createAttributeMap());
	}

	@Override
	public XLog createLog(XAttributeMap attributes) {
		XAttributeMap attributeMap = createAttributeMap();
		attributeMap.putAll(attributes);
		return new XLogImpl(attributeMap);
	}

	@Override
	public XTrace createTrace() {
		return new XTraceImpl(createAttributeMap());
	}

	@Override
	public XTrace createTrace(XAttributeMap attributes) {
		XAttributeMap attributeMap = createAttributeMap();
		attributeMap.putAll(attributes);
		return new XTraceImpl(attributeMap);
	}

	@Override
	public XEvent createEvent() {
		return new PumpEventImpl(createAttributeMap());
	}

	@Override
	public XEvent createEvent(XAttributeMap attributes) {
		XAttributeMap attributeMap = createAttributeMap();
		attributeMap.putAll(attributes);
		return new PumpEventImpl(attributeMap);
	}

	@Override
	public XEvent createEvent(XID id, XAttributeMap attributes) {
		throw new UnsupportedOperationException("Cannot create an XEvent with a pre-defined ID");
	}

	@Override
	public XAttributeMap createAttributeMap() {
		return new PumpAttributeMapImpl();
	}

	private final Integer getInternalKey(String key) {
		return targetStore.getAttributeKeyPool().put(key);
	}

	@Override
	public XAttributeBoolean createAttributeBoolean(String key, boolean value, XExtension extension) {
		return new PumpBooleanImpl(getInternalKey(key), value, extension, targetStore, null);
	}

	@Override
	public XAttributeContinuous createAttributeContinuous(String key, double value, XExtension extension) {
		return new PumpContinuousImpl(getInternalKey(key), value, extension, targetStore, null);
	}

	@Override
	public XAttributeDiscrete createAttributeDiscrete(String key, long value, XExtension extension) {
		return new PumpDiscreteImpl(getInternalKey(key), value, extension, targetStore, null);
	}

	@Override
	public XAttributeLiteral createAttributeLiteral(String key, String value, XExtension extension) {
		return new PumpLiteralImpl(getInternalKey(key), value, extension, targetStore, null);
	}

	@Override
	public XAttributeTimestamp createAttributeTimestamp(String key, Date value, XExtension extension) {
		return new PumpTimestampImpl(getInternalKey(key), value, extension, targetStore, null);
	}

	@Override
	public XAttributeTimestamp createAttributeTimestamp(String key, long millis, XExtension extension) {
		return new PumpTimestampImpl(getInternalKey(key), millis, extension, targetStore, null);
	}

	public XAttributeContainer createAttributeContainer(String key, XExtension extension) {
		return new PumpContainerImpl(getInternalKey(key), extension, targetStore, null);
	}

	public XAttributeList createAttributeList(String key, XExtension extension) {
		return new PumpListImpl(getInternalKey(key), extension, targetStore, null);
	}

	@Override
	public XAttributeID createAttributeID(String key, XID value, XExtension extension) {
		return new PumpIDImpl(getInternalKey(key), value, extension, targetStore, null);
	}

}