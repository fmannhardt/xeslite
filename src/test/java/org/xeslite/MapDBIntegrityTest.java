package org.xeslite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map.Entry;

import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.extension.std.XLifecycleExtension;
import org.deckfour.xes.extension.std.XTimeExtension;
import org.deckfour.xes.factory.XFactory;
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
import org.junit.Test;
import org.xeslite.external.XFactoryExternalStore;

public class MapDBIntegrityTest {

	@Test
	public void testIntegrityInMemoryStore() {
		testIntegrity(new XFactoryExternalStore.InMemoryStoreImpl(), false);
	}

	@Test
	public void testIntegrityMapDBDisk() {
		testIntegrity(new XFactoryExternalStore.MapDBDiskImpl());
	}

	@Test
	public void testIntegrityMapDBDiskWithoutCache() {
		testIntegrity(new XFactoryExternalStore.MapDBDiskWithoutCacheImpl());
	}

	@Test
	public void testIntegrityMapDBDiskLowMemory() {
		testIntegrity(new XFactoryExternalStore.MapDBDiskSequentialAccessImpl());
	}

	@Test
	public void testIntegrityMapDBDiskLowMemoryWithoutCache() {
		testIntegrity(new XFactoryExternalStore.MapDBDiskSequentialAccessWithoutCacheImpl());
	}

	public void testIntegrity(XFactory factory) {
		testIntegrity(factory, true);
	}

	public void testIntegrity(XFactory factory, boolean supportsContainers) {

		XAttributeMap logAttributes = factory.createAttributeMap();
		XAttributeLiteral logName = factory.createAttributeLiteral(XConceptExtension.KEY_NAME, "Test",
				XConceptExtension.instance());
		logAttributes.put(XConceptExtension.KEY_NAME, logName);
		XLog log = factory.createLog(logAttributes);

		XAttributeBoolean attributeBoolean = factory.createAttributeBoolean("Boolean", true, null);
		XAttributeContinuous attributeContinuous = factory.createAttributeContinuous("Continuous", 1.0d, null);
		XAttributeDiscrete attributeDiscrete = factory.createAttributeDiscrete("Discrete", 1, null);
		XAttributeID attributeID = factory.createAttributeID("ID", new XID(), null);
		XAttributeLiteral attributeLiteral = factory.createAttributeLiteral("Literal", "Literal", null);
		XAttributeTimestamp attributeTimestamp = factory.createAttributeTimestamp("Timestamp", -1l, null);

		XAttributeList attributeList = factory.createAttributeList("List", null);
		attributeList.addToCollection(factory.createAttributeBoolean("Boolean2", false, null));
		attributeList.addToCollection(factory.createAttributeBoolean("Boolean3", true, null));
		XAttributeContainer attributeContainer = factory.createAttributeContainer("Container", null);

		// Cached values
		XAttributeLiteral attributeConceptExtension = factory.createAttributeLiteral(XConceptExtension.KEY_NAME,
				XConceptExtension.KEY_NAME, XConceptExtension.instance());
		XAttributeTimestamp attributeTimeExtension = factory.createAttributeTimestamp(XTimeExtension.KEY_TIMESTAMP, 2l,
				XTimeExtension.instance());
		XAttributeLiteral attributeLifecycleExtension = factory.createAttributeLiteral(
				XLifecycleExtension.KEY_TRANSITION, XLifecycleExtension.StandardModel.COMPLETE.getEncoding(),
				XLifecycleExtension.instance());

		// Use the standard XES implementation as reference
		XAttributeMap attributeMap = new XAttributeMapImpl();
		attributeMap.put(attributeBoolean.getKey(), attributeBoolean);
		attributeMap.put(attributeContinuous.getKey(), attributeContinuous);
		attributeMap.put(attributeDiscrete.getKey(), attributeDiscrete);
		attributeMap.put(attributeLiteral.getKey(), attributeLiteral);
		attributeMap.put(attributeID.getKey(), attributeID);
		attributeMap.put(attributeTimestamp.getKey(), attributeTimestamp);
		if (supportsContainers) {
			attributeMap.put(attributeList.getKey(), attributeList);
			attributeMap.put(attributeContainer.getKey(), attributeContainer);
		}
		attributeMap.put(attributeConceptExtension.getKey(), attributeConceptExtension);
		attributeMap.put(attributeTimeExtension.getKey(), attributeTimeExtension);
		attributeMap.put(attributeLifecycleExtension.getKey(), attributeLifecycleExtension);

		// Created with attributes
		XTrace trace1 = factory.createTrace(attributeMap);
		XEvent t1_e1 = factory.createEvent(attributeMap);
		XEvent t1_e2 = factory.createEvent(attributeMap);
		XEvent t1_e3 = factory.createEvent(attributeMap);

		trace1.add(t1_e1);
		trace1.add(t1_e2);
		trace1.add(t1_e3);
		log.add(trace1);

		// Attributes added later
		XTrace trace2 = factory.createTrace();
		XEvent t2_e1 = factory.createEvent();
		XEvent t2_e2 = factory.createEvent();
		XEvent t2_e3 = factory.createEvent();

		trace2.getAttributes().put(attributeBoolean.getKey(), attributeBoolean);
		trace2.getAttributes().put(attributeContinuous.getKey(), attributeContinuous);
		trace2.getAttributes().put(attributeDiscrete.getKey(), attributeDiscrete);
		trace2.getAttributes().put(attributeLiteral.getKey(), attributeLiteral);
		trace2.getAttributes().put(attributeID.getKey(), attributeID);
		trace2.getAttributes().put(attributeTimestamp.getKey(), attributeTimestamp);
		if (supportsContainers) {
			trace2.getAttributes().put(attributeList.getKey(), attributeList);
			trace2.getAttributes().put(attributeContainer.getKey(), attributeContainer);
		}
		trace2.getAttributes().put(attributeConceptExtension.getKey(), attributeConceptExtension);
		trace2.getAttributes().put(attributeTimeExtension.getKey(), attributeTimeExtension);
		trace2.getAttributes().put(attributeLifecycleExtension.getKey(), attributeLifecycleExtension);

		t2_e1.getAttributes().put(attributeBoolean.getKey(), attributeBoolean);
		t2_e1.getAttributes().put(attributeContinuous.getKey(), attributeContinuous);
		t2_e1.getAttributes().put(attributeDiscrete.getKey(), attributeDiscrete);
		t2_e1.getAttributes().put(attributeLiteral.getKey(), attributeLiteral);
		t2_e1.getAttributes().put(attributeID.getKey(), attributeID);
		t2_e1.getAttributes().put(attributeTimestamp.getKey(), attributeTimestamp);
		if (supportsContainers) {
			t2_e1.getAttributes().put(attributeList.getKey(), attributeList);
			t2_e1.getAttributes().put(attributeContainer.getKey(), attributeContainer);
		}
		t2_e1.getAttributes().put(attributeConceptExtension.getKey(), attributeConceptExtension);
		t2_e1.getAttributes().put(attributeTimeExtension.getKey(), attributeTimeExtension);
		t2_e1.getAttributes().put(attributeLifecycleExtension.getKey(), attributeLifecycleExtension);

		t2_e2.getAttributes().put(attributeBoolean.getKey(), attributeBoolean);
		t2_e2.getAttributes().put(attributeContinuous.getKey(), attributeContinuous);
		t2_e2.getAttributes().put(attributeDiscrete.getKey(), attributeDiscrete);
		t2_e2.getAttributes().put(attributeLiteral.getKey(), attributeLiteral);
		t2_e2.getAttributes().put(attributeID.getKey(), attributeID);
		t2_e2.getAttributes().put(attributeTimestamp.getKey(), attributeTimestamp);
		if (supportsContainers) {
			t2_e2.getAttributes().put(attributeList.getKey(), attributeList);
			t2_e2.getAttributes().put(attributeContainer.getKey(), attributeContainer);
		}
		t2_e2.getAttributes().put(attributeConceptExtension.getKey(), attributeConceptExtension);
		t2_e2.getAttributes().put(attributeTimeExtension.getKey(), attributeTimeExtension);
		t2_e2.getAttributes().put(attributeLifecycleExtension.getKey(), attributeLifecycleExtension);

		t2_e3.getAttributes().put(attributeBoolean.getKey(), attributeBoolean);
		t2_e3.getAttributes().put(attributeContinuous.getKey(), attributeContinuous);
		t2_e3.getAttributes().put(attributeDiscrete.getKey(), attributeDiscrete);
		t2_e3.getAttributes().put(attributeLiteral.getKey(), attributeLiteral);
		t2_e3.getAttributes().put(attributeID.getKey(), attributeID);
		t2_e3.getAttributes().put(attributeTimestamp.getKey(), attributeTimestamp);
		if (supportsContainers) {
			t2_e3.getAttributes().put(attributeList.getKey(), attributeList);
			t2_e3.getAttributes().put(attributeContainer.getKey(), attributeContainer);
		}
		t2_e3.getAttributes().put(attributeConceptExtension.getKey(), attributeConceptExtension);
		t2_e3.getAttributes().put(attributeTimeExtension.getKey(), attributeTimeExtension);
		t2_e3.getAttributes().put(attributeLifecycleExtension.getKey(), attributeLifecycleExtension);

		trace2.add(t2_e1);
		trace2.add(t2_e2);
		trace2.add(t2_e3);
		log.add(trace2);

		assertNotNull(log);
		assertTrue(log.hasAttributes());
		assertEquals(logName, log.getAttributes().get(XConceptExtension.KEY_NAME));

		assertNotNull(log.get(0));
		assertNotNull(log.get(1));

		assertNotNull(log.get(0).get(0));
		assertNotNull(log.get(0).get(1));
		assertNotNull(log.get(0).get(2));

		assertTrue(log.get(0).get(0).hasAttributes());
		assertTrue(log.get(0).get(1).hasAttributes());
		assertTrue(log.get(0).get(2).hasAttributes());

		assertAttributes(log.get(0), attributeMap.values());

		assertAttributes(log.get(0).get(0), attributeMap.values());
		assertAttributes(log.get(0).get(1), attributeMap.values());
		assertAttributes(log.get(0).get(2), attributeMap.values());

		assertNotNull(log.get(1).get(0));
		assertNotNull(log.get(1).get(1));
		assertNotNull(log.get(1).get(2));

		assertTrue(log.get(1).get(0).hasAttributes());
		assertTrue(log.get(1).get(1).hasAttributes());
		assertTrue(log.get(1).get(2).hasAttributes());

		assertAttributes(log.get(1), attributeMap.values());

		assertAttributes(log.get(1).get(0), attributeMap.values());
		assertAttributes(log.get(1).get(1), attributeMap.values());
		assertAttributes(log.get(1).get(2), attributeMap.values());

		for (XTrace t : log) {
			for (XEvent e : t) {
				for (Entry<String, XAttribute> a : e.getAttributes().entrySet()) {
					if (a.getValue() instanceof XAttributeTimestamp) {
						a.setValue(factory.createAttributeTimestamp(a.getKey(), 1000l, a.getValue().getExtension()));
					} else if (a.getKey().equals(XConceptExtension.KEY_NAME) || a.getKey().equals("Literal")) {
						a.setValue(factory.createAttributeLiteral(a.getKey(), "Changed", a.getValue().getExtension()));
					} else if (a.getKey().equals(XTimeExtension.KEY_TIMESTAMP)) {
						a.setValue(factory.createAttributeTimestamp(a.getKey(), 1000l, a.getValue().getExtension()));
					} else if (a.getKey().equals(XLifecycleExtension.KEY_TRANSITION)) {
						a.setValue(factory.createAttributeLiteral(XLifecycleExtension.KEY_TRANSITION,
								XLifecycleExtension.StandardModel.START.getEncoding(), XLifecycleExtension.instance()));
					}
				}
			}
		}

		// Access with collection view
		for (XTrace t : log) {
			for (XEvent e : t) {
				for (XAttribute a : e.getAttributes().values()) {
					if (a instanceof XAttributeTimestamp) {
						assertEquals(1000l, ((XAttributeTimestamp) a).getValueMillis());
					} else if (a.getKey().equals(XConceptExtension.KEY_NAME) || a.getKey().equals("Literal")) {
						assertEquals("Changed", ((XAttributeLiteral) a).getValue());
					} else if (a.getKey().equals(XTimeExtension.KEY_TIMESTAMP)) {
						assertEquals(1000l, ((XAttributeTimestamp) a).getValueMillis());
					} else if (a.getKey().equals(XLifecycleExtension.KEY_TRANSITION)) {
						assertEquals(XLifecycleExtension.StandardModel.START.getEncoding(),
								((XAttributeLiteral) a).getValue());
					}
				}
			}
		}

		// Access with get
		for (XTrace t : log) {
			for (XEvent e : t) {
				assertEquals(1000l,
						((XAttributeTimestamp) e.getAttributes().get(XTimeExtension.KEY_TIMESTAMP)).getValueMillis());
				assertEquals(1000l, ((XAttributeTimestamp) e.getAttributes().get("Timestamp")).getValueMillis());
				assertEquals("Changed",
						((XAttributeLiteral) e.getAttributes().get(XConceptExtension.KEY_NAME)).getValue());
				assertEquals(XLifecycleExtension.StandardModel.START.getEncoding(),
						((XAttributeLiteral) e.getAttributes().get(XLifecycleExtension.KEY_TRANSITION)).getValue());
			}
		}

		// Mutate
		for (XTrace t : log) {
			for (XEvent e : t) {
				((XAttributeTimestamp) e.getAttributes().get(XTimeExtension.KEY_TIMESTAMP)).setValueMillis(1);
				assertEquals(1l,
						((XAttributeTimestamp) e.getAttributes().get(XTimeExtension.KEY_TIMESTAMP)).getValueMillis());
			}
		}

		XEvent replaceEvent = factory.createEvent();

		// Iterator previous/set
		for (XTrace t : log) {
			for (ListIterator<XEvent> iterator = t.listIterator(); iterator.hasNext();) {
				iterator.next();
				iterator.set(replaceEvent);
				if (iterator.hasPrevious()) {
					assertEquals(replaceEvent, t.get(iterator.previousIndex()));
				}
			}
		}

		XEvent previousEvent = factory.createEvent();

		// Iterator previous/set
		for (XTrace t : log) {
			for (ListIterator<XEvent> iterator = t.listIterator(t.size()); iterator.hasPrevious();) {
				iterator.previous();
				iterator.set(previousEvent);
				if (iterator.hasNext()) {
					assertEquals(previousEvent, t.get(iterator.nextIndex()));
				}
			}
		}

		// Remove via iterator
		for (XTrace t : log) {
			for (Iterator<XEvent> iterator = t.iterator(); iterator.hasNext();) {
				assertEquals(previousEvent, iterator.next());
				if (iterator.hasNext()) {
					iterator.remove();
				}
			}
			assertEquals(1, t.size());
		}
	}

	private void assertAttributes(XAttributable attributable, Collection<XAttribute> attributes) {
		for (XAttribute a : attributes) {
			XAttribute storedAttribute = attributable.getAttributes().get(a.getKey());
			if (a instanceof XAttributeList) {
				// equals is broken for XAttributeList
				assertTrue("Wrong type", storedAttribute instanceof XAttributeList);
				assertEquals("Wrong size", ((XAttributeList) a).getCollection().size(),
						((XAttributeList) storedAttribute).getCollection().size());
				assertTrue("Wrong elements", ((XAttributeList) storedAttribute).getCollection()
						.containsAll(((XAttributeList) a).getCollection()));
			} else if (a instanceof XAttributeContainer) {
				// equals is broken for XAttributeList
				assertTrue("Wrong type", storedAttribute instanceof XAttributeContainer);
				assertEquals("Wrong size", ((XAttributeContainer) a).getCollection().size(),
						((XAttributeContainer) storedAttribute).getCollection().size());
				assertTrue("Wrong elements", ((XAttributeContainer) storedAttribute).getCollection()
						.containsAll(((XAttributeContainer) a).getCollection()));
			} else {
				assertEquals(a, storedAttribute);
			}
		}
	}

}
