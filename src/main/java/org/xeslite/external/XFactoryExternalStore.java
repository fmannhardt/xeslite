package org.xeslite.external;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.deckfour.xes.extension.XExtension;
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
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XAttributeTimestamp;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.impl.XAttributeMapImpl;
import org.xeslite.external.MapDBStore.Builder;

import com.google.common.collect.ImmutableList;

/**
 * Factory storing attributes of {@link XAttributable} XES objects in a,
 * possibly external, {@link ExternalStore}. This implementation stores
 * attributes of {@link XEvent}, {@link XTrace} objects in a
 * {@link ExternalStore} either on a file-backed storage or a memory-backed
 * storage.
 * <p>
 * Use one of the embedded sub-classes in this class to create a version that
 * fits your needs:
 * <ul>
 * <li>{@link MapDBDiskImpl}
 * <li>{@link MapDBDiskWithoutCacheImpl}
 * <li>{@link MapDBDiskSequentialAccessImpl}
 * <li>{@link MapDBDiskSequentialAccessWithoutCacheImpl}
 * <li>{@link InMemoryStoreImpl}
 * </ul>
 * <p>
 * PLEASE NOTE: Attributes are generally immutable in this implementation, as
 * they may be serialized and re-created at any time.
 * 
 * @author F. Mannhardt
 * 
 */
public abstract class XFactoryExternalStore implements XFactory {

	/**
	 * A XES Factory that stores XAttributes with MapDB on disk.
	 * <p>
	 * This version does store common attributes like 'concept:name',
	 * 'lifecycle:transition', 'time:timestamp' in memory.
	 * 
	 * @author F. Mannhardt
	 * 
	 */
	public static class MapDBDiskImpl extends XFactoryExternalStore {

		public static void register() {
			if (!containsFactory(MapDBDiskImpl.class)) {
				XFactoryRegistry.instance().register(new MapDBDiskImpl());
			}
		}
		
		private final Builder dbBuilder;		
		private ExternalStore attributeStore;

		public MapDBDiskImpl() {
			this(new MapDBStore.Builder());
		}

		public MapDBDiskImpl(MapDBDatabase database) {
			this(new MapDBStore.Builder().withDatabase(database));
		}

		public MapDBDiskImpl(MapDBStore.Builder dbBuilder) {
			super();
			this.dbBuilder = dbBuilder;			
		}

		@Override
		protected synchronized final ExternalStore getStore() {
			if (attributeStore != null) {
				return attributeStore;	
			} else {
				attributeStore = dbBuilder.build();
				return attributeStore;
			}			
		}

		@Override
		public String getName() {
			return "XESLite: MapDB (with Cache)";
		}

		@Override
		public String getDescription() {
			return "A XES Factory that stores XAttributes with MapDB on disk. This version does store common attributes like 'concept:name', 'lifecycle:transition', 'time:timestamp' in memory.";
		}

	}

	/**
	 * A XES Factory that stores XAttributes with MapDB on disk. This version
	 * does <b>NOT</b> store common attributes like 'concept:name',
	 * 'lifecycle:transition', 'time:timestamp' in memory.
	 * 
	 * @author F. Mannhardt
	 * 
	 */
	public static class MapDBDiskWithoutCacheImpl extends MapDBDiskImpl {

		public static void register() {
			if (!containsFactory(MapDBDiskWithoutCacheImpl.class)) {
				XFactoryRegistry.instance().register(new MapDBDiskWithoutCacheImpl());
			}
		}

		public MapDBDiskWithoutCacheImpl() {
			super();
		}

		public MapDBDiskWithoutCacheImpl(Builder dbBuilder) {
			super(dbBuilder);
		}

		public MapDBDiskWithoutCacheImpl(MapDBDatabase database) {
			super(database);
		}

		@Override
		public String getName() {
			return "XESLite: MapDB (without Cache)";
		}

		@Override
		public String getDescription() {
			return "A XES Factory that stores XAttributes with MapDB on disk. This version does NOT store common attributes like 'concept:name', 'lifecycle:transition', 'time:timestamp' in memory. ";
		}

		@Override
		public XEvent createEvent() {
			return new XEventBareExternalImpl(getStore());
		}

		@Override
		XEvent openEvent(long externalId) {
			return new XEventBareExternalImpl(externalId, null, getStore());
		}

		@Override
		public XEvent createEvent(XAttributeMap attributes) {
			return new XEventBareExternalImpl(attributes, getStore());
		}

	}

	/**
	 * A XES Factory that stores XAttributes with MapDB on disk. This version is
	 * optimized for sequential access of events, performance will degraded when
	 * using random-access methods of XTrace.
	 * <p>
	 * This version does store common attributes like 'concept:name',
	 * 'lifecycle:transition', 'time:timestamp' in memory.
	 * 
	 * @author F. Mannhardt
	 * 
	 */
	public static class MapDBDiskSequentialAccessImpl extends MapDBDiskImpl {

		public static void register() {
			if (!containsFactory(MapDBDiskSequentialAccessImpl.class)) {
				XFactoryRegistry.instance().register(new MapDBDiskSequentialAccessImpl());
			}
		}

		public MapDBDiskSequentialAccessImpl() {
			super();
		}

		public MapDBDiskSequentialAccessImpl(Builder dbBuilder) {
			super(dbBuilder);
		}

		public MapDBDiskSequentialAccessImpl(MapDBDatabase database) {
			super(database);
		}

		@Override
		public String getName() {
			return "XESLite: MapDB (Compressed, Sequential)";
		}

		@Override
		public String getDescription() {
			return "A XES Factory that stores XAttributes with MapDB on disk. This version is optimized for sequential access of events, performance will degraded when using random-access methods of XTrace. "
					+ "This version does store common attributes like 'concept:name', 'lifecycle:transition', 'time:timestamp' in memory. ";
		}

		@Override
		public XTrace createTrace() {
			return new XTraceCompressedExternalImpl(true, getStore());
		}

		@Override
		public XTrace createTrace(XAttributeMap attributes) {
			return new XTraceCompressedExternalImpl(true, attributes, getStore());
		}

		@Override
		public XTrace createTrace(Collection<XEvent> events) {
			return new XTraceCompressedExternalImpl(true, getStore(), events);
		}

		@Override
		public XEvent createEvent() {
			return new XEventCachingExternalImpl(getStore());
		}

		@Override
		public XEvent createEvent(XAttributeMap attributes) {
			return new XEventCachingExternalImpl(attributes, getStore());
		}

	}

	/**
	 * A XES Factory that stores XAttributes with MapDB on disk. This version is
	 * optimized for sequential access of events, performance will degraded when
	 * using random-access methods of XTrace.
	 * <p>
	 * This version does <b>NOT</b> store common attributes like 'concept:name',
	 * 'lifecycle:transition', 'time:timestamp' in memory.
	 * 
	 * @author F. Mannhardt
	 * 
	 */
	public static class MapDBDiskSequentialAccessWithoutCacheImpl extends MapDBDiskSequentialAccessImpl {

		public static void register() {
			if (!containsFactory(MapDBDiskSequentialAccessWithoutCacheImpl.class)) {
				XFactoryRegistry.instance().register(new MapDBDiskSequentialAccessWithoutCacheImpl());
			}
		}

		public MapDBDiskSequentialAccessWithoutCacheImpl() {
			super();
		}

		public MapDBDiskSequentialAccessWithoutCacheImpl(Builder dbBuilder) {
			super(dbBuilder);
		}

		public MapDBDiskSequentialAccessWithoutCacheImpl(MapDBDatabase database) {
			super(database);
		}

		@Override
		public String getName() {
			return "XESLite: MapDB Optimized for memory & sequential access";
		}

		@Override
		public String getDescription() {
			return "A XES Factory that stores XAttributes with MapDB on disk."
					+ " This version is optimized for sequential access of events, performance will degraded when using random-access methods of XTrace."
					+ " This version does NOT store common attributes like 'concept:name', 'lifecycle:transition', 'time:timestamp' in memory.";
		}

		@Override
		public XTrace createTrace() {
			return new XTraceCompressedExternalImpl(false, getStore());
		}

		@Override
		public XTrace createTrace(XAttributeMap attributes) {
			return new XTraceCompressedExternalImpl(false, attributes, getStore());
		}

		@Override
		public XTrace createTrace(Collection<XEvent> events) {
			return new XTraceCompressedExternalImpl(false, getStore(), events);
		}

		@Override
		public XEvent createEvent() {
			return new XEventBareExternalImpl(getStore());
		}

		@Override
		public XEvent createEvent(XAttributeMap attributes) {
			return new XEventBareExternalImpl(attributes, getStore());
		}

	}

	/**
	 * A XES factory that stores the attributes in an optimized column-format in
	 * the heap memory. This factory requires all attributes to have a
	 * consistent type across the whole event log.
	 * 
	 * @author F. Mannhardt
	 *
	 */
	public static class InMemoryStoreImpl extends XFactoryExternalStore {
		
		public static void register() {
			if (!containsFactory(InMemoryStoreImpl.class)) {
				XFactoryRegistry.instance().register(new InMemoryStoreImpl());
			}
		}

		private final InMemoryStore attributeStore;

		public InMemoryStoreImpl() {
			super();
			attributeStore = new InMemoryStore();
		}

		@Override
		public XTrace createTrace() {
			return new XTraceCompressedExternalImpl(true, getStore());
		}

		@Override
		public XTrace createTrace(XAttributeMap attributes) {
			return new XTraceCompressedExternalImpl(true, attributes, getStore());
		}

		@Override
		public XTrace createTrace(Collection<XEvent> events) {
			return new XTraceCompressedExternalImpl(true, getStore(), events);
		}

		@Override
		public XEvent createEvent() {
			return new XEventCachingExternalImpl(getStore());
		}

		@Override
		XEvent openEvent(long externalId) {
			return new XEventCachingExternalImpl(externalId, null, getStore());
		}

		@Override
		public XEvent createEvent(XAttributeMap attributes) {
			return new XEventCachingExternalImpl(attributes, getStore());
		}

		@Override
		protected final ExternalStore getStore() {
			return attributeStore;
		}

		@Override
		public String getName() {
			return "XESLite: In-Memory Store";
		}

		@Override
		public String getDescription() {
			return "XESLite: In-Memory Store, does require attributes to have a consistent type.";
		}

	}

	/**
	 * Specialized implementation for storing aligned logs.
	 * 
	 * @author F. Mannhardt
	 *
	 */
	public static class InMemoryStoreAlignmentAwareImpl extends XFactoryExternalStore {

		private final InMemoryStore attributeStore;

		public InMemoryStoreAlignmentAwareImpl() {
			super();
			attributeStore = new InMemoryStore();
		}

		@Override
		public XTrace createTrace() {
			return new XTraceExternalImpl(getStore());
		}

		@Override
		public XTrace createTrace(XAttributeMap attributes) {
			return new XTraceExternalImpl(attributes, getStore());
		}

		@Override
		public XTrace createTrace(Collection<XEvent> events) {
			return new XTraceExternalImpl(getStore(), events);
		}

		@Override
		public XEvent createEvent() {
			return new XAlignmentEventExternalImpl(getStore());
		}

		@Override
		public XEvent createEvent(XAttributeMap attributes) {
			return new XAlignmentEventExternalImpl(attributes, getStore());
		}

		@Override
		protected final ExternalStore getStore() {
			return attributeStore;
		}

		@Override
		public String getName() {
			return "XESLite: In-Memory Store (Alignments)";
		}

		@Override
		public String getDescription() {
			return "XESLite: In-Memory Store optimized for storing alignments.";
		}

	}

	protected abstract ExternalStore getStore();

	public static boolean containsFactory(Class<? extends XFactoryExternalStore> clazz) {
		for (XFactory f : XFactoryRegistry.instance().getAvailable()) {
			if (clazz == f.getClass()) {
				return true;
			}
		}
		return false;
	}

	protected Integer intern(String key) {
		return getStore().getAttributeKeyPool().put(key);
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
		return URI.create("https://github.com/fmannhardt/xeslite");
	}

	@Override
	public String getVendor() {
		return "https://github.com/fmannhardt/xeslite";
	}

	@Override
	public XLog createLog() {
		return new XLogExternalImpl(getStore());
	}

	@Override
	public XLog createLog(XAttributeMap attributes) {
		return new XLogExternalImpl(attributes, getStore());
	}

	public XLog createLog(Collection<XTrace> traces) {
		return new XLogExternalImpl(getStore(), traces);
	}

	XLog openLog(long externalId) {
		return new XLogExternalImpl(externalId, null, getStore(), ImmutableList.<XTrace> of(), true);
	}

	@Override
	public XTrace createTrace() {
		return new XTraceExternalImpl(getStore());
	}

	public XTrace createTrace(Collection<XEvent> events) {
		return new XTraceExternalImpl(getStore(), events);
	}

	@Override
	public XTrace createTrace(XAttributeMap attributes) {
		return new XTraceExternalImpl(attributes, getStore());
	}

	XTrace openTrace(long externalId, Collection<XEvent> events) {
		return new XTraceExternalImpl(externalId, null, getStore(), events);
	}

	@Override
	public XEvent createEvent() {
		return new XEventCachingExternalImpl(getStore());
	}

	@Override
	public XEvent createEvent(XAttributeMap attributes) {
		return new XEventCachingExternalImpl(attributes, getStore());
	}

	@Override
	public XEvent createEvent(XID id, XAttributeMap attributes) {
		throw new UnsupportedOperationException("Can not create an XEvent with a pre-defined ID");
	}

	XEvent openEvent(long externalId) {
		return new XEventCachingExternalImpl(externalId, null, getStore());
	}

	@Override
	public XAttributeMap createAttributeMap() {
		return new XAttributeMapImpl();
	}

	@Override
	public XAttributeBoolean createAttributeBoolean(String key, boolean value, XExtension extension) {
		return new XAttributeBooleanExternalImpl(intern(key), value, extension, getStore(), null);
	}

	@Override
	public XAttributeContinuous createAttributeContinuous(String key, double value, XExtension extension) {
		return new XAttributeContinuousExternalImpl(intern(key), value, extension, getStore(), null);
	}

	@Override
	public XAttributeDiscrete createAttributeDiscrete(String key, long value, XExtension extension) {
		return new XAttributeDiscreteExternalImpl(intern(key), value, extension, getStore(), null);
	}

	@Override
	public XAttributeLiteral createAttributeLiteral(String key, String value, XExtension extension) {
		return new XAttributeLiteralExternalImpl(intern(key), value, extension, getStore(), null);
	}

	@Override
	public XAttributeTimestamp createAttributeTimestamp(String key, Date value, XExtension extension) {
		return new XAttributeTimestampExternalImpl(intern(key), value, extension, getStore(), null);
	}

	@Override
	public XAttributeTimestamp createAttributeTimestamp(String key, long millis, XExtension extension) {
		return new XAttributeTimestampExternalImpl(intern(key), millis, extension, getStore(), null);
	}

	@Override
	public XAttributeID createAttributeID(String key, XID value, XExtension extension) {
		return new XAttributeIDExternalImpl(intern(key), value, extension, getStore(), null);
	}

	@Override
	public XAttributeContainer createAttributeContainer(String key, XExtension extension) {
		return new XAttributeContainerExternalImpl(intern(key), extension, getStore(), null);
	}

	@Override
	public XAttributeList createAttributeList(String key, XExtension extension) {
		return new XAttributeListExternalImpl(intern(key), extension, getStore(), null);
	}

	public void startPump() {
		if (getStore().isPumping()) {
			throw new IllegalStateException("Pump has already been started!");
		}
		getStore().startPump();
	}

	public boolean isPumping() {
		return getStore().isPumping();
	}

	public XEvent pumpEvent(XEvent pumpEvent) {
		if (pumpEvent.hasAttributes()) {
			List<XAttribute> pumpValues = getAttributeList(pumpEvent);
			pumpMetaAttributes(pumpValues);
			XEvent event = createEvent();
			getStore().getPumpService().pumpAttributes(event, pumpValues);
			return event;
		} else {
			return createEvent();
		}
	}

	public XTrace pumpTrace(XTrace pumpTrace) {
		if (pumpTrace.hasAttributes()) {
			// Assumes events already have been pumped, only handle our own
			// attributes
			List<XAttribute> pumpValues = getAttributeList(pumpTrace);
			pumpMetaAttributes(pumpValues);
			XTrace trace = createTrace(pumpTrace);
			getStore().getPumpService().pumpAttributes(trace, pumpValues);
			return trace;
		} else {
			return createTrace(pumpTrace);
		}
	}

	public XLog pumpLog(XLog pumpLog) {
		if (pumpLog.hasAttributes()) {
			XLog log = createLog(pumpLog);
			for (XAttribute attr : getAttributeList(pumpLog)) {
				log.getAttributes().put(attr.getKey(), attr);
			}
			copyLogMetadata(pumpLog, log);
			return log;
		} else {
			XLog log = createLog(pumpLog);
			copyLogMetadata(pumpLog, log);
			return log;
		}
	}

	protected List<XAttribute> getAttributeList(XAttributable attributable) {
		List<XAttribute> pumpValues;
		if (attributable.getAttributes() instanceof PumpAttributeMapImpl) {
			pumpValues = ((PumpAttributeMapImpl) attributable.getAttributes()).values();
		} else {
			pumpValues = new ArrayList<>(attributable.getAttributes().values());
		}
		return pumpValues;
	}

	private static void copyLogMetadata(XLog source, XLog target) {
		target.getClassifiers().addAll(source.getClassifiers());
		target.getExtensions().addAll(source.getExtensions());
		target.getGlobalEventAttributes().addAll(source.getGlobalEventAttributes());
		target.getGlobalTraceAttributes().addAll(source.getGlobalTraceAttributes());
	}

	private void pumpMetaAttributes(List<XAttribute> attributes) {
		int i = 0;
		for (XAttribute a : attributes) {
			if (a.hasAttributes()) {
				attributes.set(i, pumpAttribute(a));
			}
			i++;
		}
	}

	private XAttribute pumpAttribute(XAttribute pumpAttribute) {
		if (pumpAttribute.hasAttributes()) {
			// First handle meta attributes
			Collection<XAttribute> values = pumpAttribute.getAttributes().values();
			List<XAttribute> metaAttributes = new ArrayList<>(values.size());
			for (XAttribute metaAttribute : values) {
				metaAttributes.add(pumpAttribute(metaAttribute));
			}
			ExternalAttribute attribute = XAttributeExternalImpl.convert(getStore(), null, pumpAttribute);
			attribute.setExternalId(getStore().getIdFactory().nextId());
			getStore().getPumpService().pumpAttributes(attribute, metaAttributes);
			return attribute;
		} else {
			return pumpAttribute;
		}
	}

	public void finishPump() throws InterruptedException {
		getStore().getPumpService().finishPump();
	}

	public XFactory createPumpTransferFactory() {
		return new PumpFactoryImpl(getStore());
	}

	public void commit() {
		getStore().commit();
	}

	public void dispose() {
		getStore().dispose();
	}

	public void saveLogStructure(XLog log) {
		getStore().saveLogStructure(log);
	}

	public XLog loadLogStructure() {
		return getStore().loadLogStructure(this);
	}

}