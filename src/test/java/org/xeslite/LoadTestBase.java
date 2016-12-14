package org.xeslite;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryRegistry;
import org.deckfour.xes.in.XesXmlParser;
import org.deckfour.xes.info.XLogInfo;
import org.deckfour.xes.info.XLogInfoFactory;
import org.deckfour.xes.model.XLog;

public class LoadTestBase extends XesLiteBaseTestAbstract {

	protected void doTest(XesXmlParser parser, XFactory factory) throws FileNotFoundException, Exception {
		XFactoryRegistry.instance().setCurrentDefault(factory);

		long startTime = System.nanoTime();
		System.out.println("Parsing log: ");
		List<XLog> parsedLog = parser.parse(new GZIPInputStream(new FileInputStream("testlog.xes.gz")));
		long elapsedNanos = System.nanoTime() - startTime;
		System.out.println("Elapsed time: " + elapsedNanos / 1000000 + " ms");
		System.gc();
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
		}
		System.out.println("Memory Used: " + getMemoryUsage().getUsed() / 1024 / 1024 + " MB ");

		System.out.println("Creating log info: ");
		startTime = System.nanoTime();
		XLogInfo logInfo = XLogInfoFactory.createLogInfo(parsedLog.get(0));
		elapsedNanos = System.nanoTime() - startTime;
		System.out.println("Elapsed time: " + elapsedNanos / 1000000 + " ms");
		System.gc();
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
		}
		System.out.println("Memory Used: " + getMemoryUsage().getUsed() / 1024 / 1024 + " MB ");
		System.out.println(logInfo.toString());
	}

}
