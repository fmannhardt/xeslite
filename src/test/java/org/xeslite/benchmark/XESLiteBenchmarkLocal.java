package org.xeslite.benchmark;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryNaiveImpl;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.xeslite.common.XUtils;
import org.xeslite.dfa.XLogDFAXmlParser;
import org.xeslite.external.MapDBDatabaseImpl;
import org.xeslite.external.MapDBStore;
import org.xeslite.external.MapDBStore.Builder;
import org.xeslite.external.XFactoryExternalStore;
import org.xeslite.parser.XesLiteXmlParser;

import com.carrotsearch.sizeof.RamUsageEstimator;
import com.google.common.base.Stopwatch;
import com.google.common.collect.FluentIterable;

public class XESLiteBenchmarkLocal {

	public interface XFactoryLoader {

		XFactory newFactory();

	}

	private final class DFALogLoader implements LogLoader {
		public XLog load(String filename) throws Exception {
			XLogDFAXmlParser parser = new XLogDFAXmlParser();
			return parser.parse(new GZIPInputStream(new FileInputStream(filename)));
		}

		public void dispose() {
			//noop
		}
	}

	private final class FactoryLogLoader implements LogLoader {

		private final XFactoryLoader factoryLoader;
		private XFactory factory;

		public FactoryLogLoader(XFactoryLoader factoryLoader) {
			this.factoryLoader = factoryLoader;
		}

		public XLog load(String filename) throws Exception {
			factory = factoryLoader.newFactory();
			if (factory instanceof XFactoryExternalStore) {
				((XFactoryExternalStore) factory).startPump();
			}
			XesLiteXmlParser parser = new XesLiteXmlParser(factory, true);
			List<XLog> result = parser.parse(new GZIPInputStream(new FileInputStream(filename)));
			if (factory instanceof XFactoryExternalStore) {
				((XFactoryExternalStore) factory).finishPump();
				((XFactoryExternalStore) factory).commit();
			}
			return FluentIterable.from(result).first().get();
		}

		public void dispose() {
			if (factory instanceof XFactoryExternalStore) {
				((XFactoryExternalStore) factory).dispose();
			}
		}
	}

	public interface LogLoader {

		XLog load(String filename) throws Exception;

		void dispose();

	}

	public class Result {
		long memoryInBytes;
		long numTraces;
		long numEvents;
		long numAttributes;
		double msLoad;
		double msIterateName;
		double msIterateAll;		
	}

	private static final int REPETITIONS = 1;

	private String testDir = "A:\\PhD Information Systems\\Artifacts\\XESLite\\Benchmark\\";

	public void testLog() {
		
		System.setErr(new PrintStream(new OutputStream() {
			
			public void write(int b) throws IOException {
			}
		}));
	
		runAllBenchmarksOn(testDir, "Sepsis Cases - Event Log.xes.gz", "Sepsis Cases");
		
		runAllBenchmarksOn(testDir, "BPI_Challenge_2012.xes.gz", "BPI 2012");
		runAllBenchmarksOn(testDir, "BPI_Challenge_2013_incidents.xes.gz", "BPI 2013 - Incidents");
		runAllBenchmarksOn(testDir, "BPI_Challenge_2013_open_problems.xes.gz", "BPI 2013 - Open Problems");

		runAllBenchmarksOn(testDir, "BPIC15_1.xes.gz", "BPI 2015 - Log 1");
		runAllBenchmarksOn(testDir, "BPIC15_2.xes.gz", "BPI 2015 - Log 2");
		runAllBenchmarksOn(testDir, "BPIC15_3.xes.gz", "BPI 2015 - Log 3");
		runAllBenchmarksOn(testDir, "BPIC15_4.xes.gz", "BPI 2015 - Log 4");
		runAllBenchmarksOn(testDir, "BPIC15_5.xes.gz", "BPI 2015 - Log 5");

		runAllBenchmarksOn(testDir, "Road_Traffic_Fine_Management_Process.xes.gz", "Road Fines");
		runAllBenchmarksOn(testDir,
				"Receipt phase of an environmental permit application process (_WABO_) CoSeLoG project.xes.gz",
				"CoSeLoG WABO");
		runAllBenchmarksOn(testDir, "Hospital-Billing-Sample.xes.gz", "Hospital Billing (Sample)");
		runAllBenchmarksOn(testDir, "Hospital_log.xes.gz", "Hospital Log (BPI'11)");

		runAllBenchmarksOn(testDir, "Hospital-Billing-Full.xes.gz", "Hospital Billing"); // OOM for OpenXES
		runAllBenchmarksOn(testDir, "Hospital-Whiteboard.xes.gz", "Hospital Whiteboard"); // OOM for OpenXES
		runAllBenchmarksOn(testDir, "BPI2016_Clicks_Logged_In.xes.gz", "BPI 2016 - Logged In"); // OOM for OpenXES

	}
	
	
	private void runAllBenchmarksOn(String dir, String logFile, String logName) {
		runAllBenchmarksOn(dir, logFile, logName, true);
	}

	private void runAllBenchmarksOn(String dir, String logFile, String logName, boolean runOpenXES) {

		String path = dir + logFile;

		if (runOpenXES) {
			try {
				printResult(logName, "OpenXES-Naive", benchmarkLog(path, new FactoryLogLoader(new XFactoryLoader() {

					public XFactory newFactory() {
						XFactoryNaiveImpl factory = new XFactoryNaiveImpl();
						factory.setUseInterner(false);
						return factory;
					}
				})));
			} catch (Exception | OutOfMemoryError e) {
				System.out.println(String.format("\"OpenXES-Naive\",\"%s\",-,-,-,-,-,-,-", logName));
			}

			try {
				printResult(logName, "OpenXES-Flyweight", benchmarkLog(path, new FactoryLogLoader(new XFactoryLoader() {

					public XFactory newFactory() {
						return new XFactoryNaiveImpl();
					}
				})));
			} catch (Exception | OutOfMemoryError e) {
				System.out.println(String.format("\"OpenXES-Flyweight\",\"%s\",-,-,-,-,-,-,-", logName));
			}
		}

		try {
			printResult(logName, "XL-DB", benchmarkLog(path, new FactoryLogLoader(new XFactoryLoader() {

				public XFactory newFactory() {
					return new XFactoryExternalStore.MapDBDiskSequentialAccessWithoutCacheImpl(getNoCacheBuilder());
				}
			})));
		} catch (Exception | OutOfMemoryError e) {
			e.printStackTrace();
			System.out.println(String.format("\"XL-DB\",\"%s\",-,-,-,-,-,-,-", logName));
		}


		try {
			printResult(logName, "XL-IM", benchmarkLog(path, new FactoryLogLoader(new XFactoryLoader() {

				public XFactory newFactory() {
					return new XFactoryExternalStore.InMemoryStoreImpl();
				}
			})));
		} catch (Exception | OutOfMemoryError e) {
			System.out.println(String.format("\"XL-IM\",\"%s\",-,-,-,-,-,-,-", logName));
		}
		
		
		try {
			printResult(logName, "XL-AT", benchmarkLog(path, new DFALogLoader()));
		} catch (Exception | OutOfMemoryError e) {
			System.out.println(String.format("\"XL-AT\",\"%s\",-,-,-,-,-,-,-", logName));
		}

	}

	private Builder getNoCacheBuilder() {
		MapDBDatabaseImpl database = new MapDBDatabaseImpl();
		database.setUseCache(false);
		return new MapDBStore.Builder().withPump().withKeyPoolShift(MapDBStore.getMaxKeyPoolShift())
				.withDatabase(database);
	}

	private void printResult(String logName, String variantName, Result result) {
		System.out.println(String.format("\"%s\",\"%s\",%d,%d,%d,%d,%.1f,%.1f,%.1f", variantName, logName, result.numTraces, result.numEvents, result.numAttributes, result.memoryInBytes,
				result.msLoad, result.msIterateName, result.msIterateAll));
	}

	private Result benchmarkLog(String filename, LogLoader loader) throws Exception {

		Result result = new Result();

		Stopwatch watch = Stopwatch.createUnstarted();

		XLog log = null;
		for (int i = 0; i < REPETITIONS; i++) {
			watch.reset().start();
			log = loader.load(filename);
			result.msLoad += watch.elapsed(TimeUnit.MILLISECONDS);
			if (i < (REPETITIONS-1)) {
				loader.dispose();
			}
		}
		result.msLoad = result.msLoad / REPETITIONS;

		System.gc();
		try {
			Thread.sleep(250);
		} catch (InterruptedException e) {
		}

		System.gc();
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
		}

		result.memoryInBytes = objectSize(log);
		
		for (int i = 0; i < REPETITIONS; i++) {
			watch.reset().start();
			for (XTrace trace : log) {
				for (XEvent event : trace) {
					String name = XUtils.getConceptName(event);
					if (name == null) { // read to avoid code optimization
						throw new RuntimeException("Invalid event");
					}
				}
			}
			result.msIterateName += watch.elapsed(TimeUnit.MILLISECONDS);
		}
		result.msIterateName = result.msIterateName / REPETITIONS;

		System.gc();
		try {
			Thread.sleep(250);
		} catch (InterruptedException e) {
		}

		for (int i = 0; i < REPETITIONS; i++) {
			watch.reset().start();
			for (XTrace trace : log) {
				for (XEvent event : trace) {
					for (XAttribute attr : event.getAttributes().values()) {
						if (attr == null) { // read to avoid code optimization
							throw new RuntimeException("Invalid event");
						}
					}
				}
			}
			result.msIterateAll += watch.elapsed(TimeUnit.MILLISECONDS);
		}
		result.msIterateAll = result.msIterateAll / REPETITIONS;

		watch.stop();
		
		for (XTrace trace : log) {
			result.numEvents += trace.size();
			result.numTraces++;
			result.numAttributes += trace.getAttributes().size();
			for (XEvent event : trace) {
				result.numAttributes += event.getAttributes().size();
			}
		}

		loader.dispose();

		return result;

	}

	private long objectSize(XLog log) {
		return RamUsageEstimator.sizeOf(log);
	}
	
}
