package org.xeslite.external;

import java.io.File;
import java.io.IOException;

import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.impl.XLogImpl;
import org.deckfour.xes.model.impl.XTraceImpl;
import org.mapdb.DB;

public interface MapDBDatabase {

	public static class IO {

		public static XLog load(File file) throws Exception {
			MapDBDatabaseImpl database = new MapDBDatabaseImpl();
			database.setTemporary(false);
			database.setDbFile(file);

			MapDBStore.Builder builder = new MapDBStore.Builder();
			builder.withDatabase(database);

			XFactoryExternalStore factory = new XFactoryExternalStore.MapDBDiskWithoutCacheImpl(builder);

			try {
				return factory.loadLogStructure();
			} catch (Exception e) {
				// Clean-up
				factory.dispose();
				throw e;
			}

		}

		public static XLog loadWithCache(File file) {
			MapDBDatabaseImpl database = new MapDBDatabaseImpl();
			database.setTemporary(false);
			database.setDbFile(file);

			MapDBStore.Builder builder = new MapDBStore.Builder();
			builder.withDatabase(database);

			XFactoryExternalStore factory = new XFactoryExternalStore.MapDBDiskImpl(builder);

			try {
				return factory.loadLogStructure();
			} catch (Exception e) {
				// Clean-up
				factory.dispose();
				throw e;
			}
		}

		public static void save(XLog log, File file) {

			// overwrite existing file to avoid mapdb opening the existing DB
			if (file.exists()) {
				file.delete();
			}

			MapDBDatabaseImpl database = new MapDBDatabaseImpl();
			database.setTemporary(false);
			database.setDbFile(file);
			database.setUseCache(false);
			database.setUseAsyncWriter(false);

			MapDBStore.Builder builder = new MapDBStore.Builder();
			builder.withPump();
			builder.withDatabase(database);

			XFactoryExternalStore factory = new XFactoryExternalStore.MapDBDiskWithoutCacheImpl(builder);

			try {
				factory.startPump();
				XLogImpl pumpLog = new XLogImpl(log.getAttributes());
				for (XTrace t : log) {
					XTraceImpl pumpTrace = new XTraceImpl(t.getAttributes());
					for (XEvent e : t) {
						pumpTrace.add(factory.pumpEvent(e));
					}
					pumpLog.add(factory.pumpTrace(pumpTrace));
				}
				XLog savedLog = factory.pumpLog(pumpLog);
				savedLog.getClassifiers().addAll(log.getClassifiers());
				savedLog.getExtensions().addAll(log.getExtensions());
				savedLog.getGlobalEventAttributes().addAll(log.getGlobalEventAttributes());
				savedLog.getGlobalTraceAttributes().addAll(log.getGlobalTraceAttributes());
				try {
					factory.finishPump();
					factory.saveLogStructure(savedLog);
				} catch (InterruptedException e) {
					return;
				}
			} finally {
				factory.dispose();
			}

		}

	}

	void createDB() throws IOException;

	DB getDB();

}
