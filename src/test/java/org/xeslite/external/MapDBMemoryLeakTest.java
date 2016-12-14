package org.xeslite.external;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Arrays;

import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.junit.Test;
import org.xeslite.common.XUtils;
import org.xeslite.external.XFactoryExternalStore.MapDBDiskImpl;

public class MapDBMemoryLeakTest {

	@Test
	public void testForMemoryLeak() {
		testMemoryConsumption(5);
	}

	private void testMemoryConsumption(int times) {
		long[] memoryUsage = new long[times];
		long[] memoryUsageMX = new long[times];

		for (int k = 0; k < times; k++) {
			System.gc();					
			MapDBDiskImpl factory = new XFactoryExternalStore.MapDBDiskImpl();
			XLog log = factory.createLog();
			log.getAttributes().put("concept:name", factory.createAttributeLiteral("concept:name", "TestLog"+k, XConceptExtension.instance()));
			for (int i = 0; i < 1000; i++) {
				XTrace trace = factory.createTrace();
				for (int j = 0; j < 100; j++) {
					XEvent event = factory.createEvent();
					event.getAttributes().put("test1", factory.createAttributeLiteral("test1", "test", null));
					event.getAttributes().put("test2", factory.createAttributeLiteral("test2", "test", null));
					event.getAttributes().put("test3", factory.createAttributeLiteral("test3", "test", null));
					event.getAttributes().put("test4", factory.createAttributeLiteral("test4", "test", null));
					event.getAttributes().put("test5", factory.createAttributeLiteral("test5", "test", null));
					event.getAttributes().put("test6", factory.createAttributeLiteral("test6", "test", null));
					trace.add(event);
				}
				log.add(trace);
			}
			System.gc();
			memoryUsage[k] = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			memoryUsageMX[k] = getMemoryUsage().getUsed();
			System.out.println(XUtils.getConceptName(log));
			factory.dispose();
		}
		System.out.println("Runtime: "+Arrays.toString(memoryUsage));
		System.out.println("MXBean: "+Arrays.toString(memoryUsageMX));
	}

	private MemoryUsage getMemoryUsage() {
		MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
		return memoryMXBean.getHeapMemoryUsage();
	}

}
