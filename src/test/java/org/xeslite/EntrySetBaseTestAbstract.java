package org.xeslite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryRegistry;
import org.deckfour.xes.in.XesXmlGZIPParser;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;

public class EntrySetBaseTestAbstract extends XesLiteBaseTestAbstract {

	protected void doTest(XFactory factory) throws FileNotFoundException, Exception {
		XFactoryRegistry.instance().setCurrentDefault(factory);
		XesXmlGZIPParser parser = new XesXmlGZIPParser(factory);

		long startTime = System.nanoTime();
		System.out.println("Parsing log: ");
		List<XLog> parsedLog = parser.parse(new FileInputStream("testlog.xes.gz"));
		assertEquals(1, parsedLog.size());
		long elapsedNanos = System.nanoTime() - startTime;
		System.out.println("Loading time: " + elapsedNanos / 1000000 + " ms");

		System.gc();
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
		}
		System.out.println("Memory Used: " + getMemoryUsage().getUsed() / 1024 / 1024 + " MB ");

		System.out.println("Iterating through attributes using entrySet: ");
		startTime = System.nanoTime();

		for (XTrace t : parsedLog.get(0)) {
			for (XEvent e : t) {
				for (Entry<String, XAttribute> entry : e.getAttributes().entrySet()) {
					String key = entry.getKey();
					XAttribute value = entry.getValue();
					assertEquals(key, value.getKey());
				}
			}
		}

		elapsedNanos = System.nanoTime() - startTime;
		System.out.println("Iterating time: " + elapsedNanos / 1000000 + " ms");
		System.gc();
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
		}
		System.out.println("Memory Used: " + getMemoryUsage().getUsed() / 1024 / 1024 + " MB ");

		System.out.println("Removing attributes using entrySet: ");
		startTime = System.nanoTime();

		for (XTrace t : parsedLog.get(0)) {
			for (XEvent e : t) {
				for (Iterator<Entry<String, XAttribute>> iterator = e.getAttributes().entrySet().iterator(); iterator
						.hasNext();) {
					Entry<String, XAttribute> entry = iterator.next();
					String key = entry.getKey();
					XAttribute value = entry.getValue();
					assertEquals(key, value.getKey());
					iterator.remove();
				}
				assertFalse(e.hasAttributes());
			}
		}

		elapsedNanos = System.nanoTime() - startTime;
		System.out.println("Iterating/Removing time: " + elapsedNanos / 1000000 + " ms");
		System.gc();
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
		}
		System.out.println("Memory Used: " + getMemoryUsage().getUsed() / 1024 / 1024 + " MB ");
	}

}
