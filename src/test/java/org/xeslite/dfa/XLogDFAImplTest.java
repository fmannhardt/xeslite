package org.xeslite.dfa;

import static org.junit.Assert.assertNotNull;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.deckfour.xes.factory.XFactoryNaiveImpl;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.junit.Test;
import org.xeslite.XesLiteBaseTestAbstract;
import org.xeslite.common.XUtils;

public class XLogDFAImplTest extends XesLiteBaseTestAbstract {

	@Test
	public void testRandomLogDFA() {
		System.out.println("Creating random log DFA");
		
		XLog reducedLog = buildRandomDFALog();
		measureMemory();
		
		for (XTrace trace: reducedLog) {
			for (XEvent event: trace) {
				assertNotNull(XUtils.getConceptName(event));
			}			
		}
	}

	private XLog buildRandomDFALog() {
		XLog randomLog = createLog(new XFactoryNaiveImpl(), 25, 100000, 0, true);
		XLogDFABuilder builder = new XLogDFABuilder();
		for (XTrace trace: randomLog) {
			builder.addTrace(trace);
		}
		XLog reducedLog = builder.build();
		return reducedLog;
	}

	private void measureMemory() {
		System.gc();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
		System.gc();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
		System.out.println("Memory Used: " + getMemoryUsage().getUsed() / 1024 / 1024 + " MB ");
	}
	
}
