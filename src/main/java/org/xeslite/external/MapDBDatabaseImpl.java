package org.xeslite.external;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.deckfour.xes.model.XAttributeLiteral;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.DBMaker.Maker;
import org.xeslite.common.XESLiteException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class MapDBDatabaseImpl implements MapDBDatabase {

	private static final int SIZE_INCREMENT = 4 * 1024 * 1024;
	/**
	 * Use 64 MB of memory assuming that each XAttribute takes up at most 144
	 * bytes, which would be approx. the case for {@link XAttributeLiteral} with
	 * a String of length 10, and the given BTreeNode size.
	 */
	private static final int DEFAULT_CACHE_SIZE = (int) (64l * 1024l * 1024l) / (30 * MapDBStore.getDefaultNodeSize());
	private static final long MIN_MEMORY_FOR_CACHING = 256l * 1024l * 1024l;

	private static final AtomicInteger databaseNumber = new AtomicInteger(0);

	private DB db;
	private File dbFile;
	private ScheduledExecutorService executorService;

	private boolean useAsyncWriter = true;
	private int asyncWriteQueueSize = 1024 * 8;
	private int asyncFlushDelay = 50;

	private boolean useCache = true;
	private boolean isTemporary = true;
	private boolean useSingleLock = false;
	private boolean useCompression = true;
	private boolean isFileDB = true;

	public synchronized void createDB() throws IOException {
		int currentDBNumber = databaseNumber.getAndIncrement();

		if (dbFile == null) {
			if (isTemporary()) {
				dbFile = File.createTempFile("xeslite-" + currentDBNumber + "-", ".db");
			} else {
				throw new XESLiteException("You need to supply a file for a non-temporary database!");
			}
		}

		ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
				.setNameFormat("XESLiteDB-" + currentDBNumber + "-Worker-Thread-%d").build();
		executorService = Executors.newScheduledThreadPool(4, threadFactory);

		Maker dbMaker;
		if (isFileDB) {
			dbMaker = DBMaker.fileDB(dbFile);
		} else {
			dbMaker = DBMaker.heapDB();
		}
		dbMaker = dbMaker
				.closeOnJvmShutdown()
				.transactionDisable()
				.cacheExecutorEnable(executorService)
				.storeExecutorEnable(executorService).allocateIncrement(SIZE_INCREMENT);
		
		if (isFileDB && supportsMmap()) {
			dbMaker.fileMmapEnable();
			dbMaker.fileMmapCleanerHackEnable();
		}
		if (isUseCompression()) {
			dbMaker.compressionEnable();
		}
		if (isUseSingleLock()) {
			dbMaker.lockSingleEnable();
		}
		if (isFileDB && isTemporary()) {
			dbMaker.deleteFilesAfterClose();
		}
		if (isUseCache()) {
			if (Runtime.getRuntime().maxMemory() >= MIN_MEMORY_FOR_CACHING) {
				dbMaker = dbMaker.cacheHashTableEnable(DEFAULT_CACHE_SIZE);
			}
		}
		if (isUseAsyncWriter()) {
			dbMaker = dbMaker.asyncWriteEnable().asyncWriteFlushDelay(asyncFlushDelay)
					.asyncWriteQueueSize(asyncWriteQueueSize);
		}
		db = dbMaker.make();
	}

	/**
	 * Adapted from MapDB, to also allow mmap for 64bit windows machines
	 */
	private static boolean supportsMmap() {
		String prop = System.getProperty("os.arch");
		if (prop != null && prop.contains("64")) {
			return true;
		}
		return false;
	}

	public DB getDB() {
		return db;
	}

	public void compact() {
		getDB().compact();
	}

	public void close() {
		getDB().close();
		executorService.shutdown();
	}

	public File getDbFile() {
		return dbFile;
	}

	public void setDbFile(File dbFile) {
		this.dbFile = dbFile;
	}

	public boolean isUseAsyncWriter() {
		return useAsyncWriter;
	}

	public MapDBDatabaseImpl setUseAsyncWriter(boolean useAsyncWriter) {
		this.useAsyncWriter = useAsyncWriter;
		return this;
	}

	public boolean isUseCache() {
		return useCache;
	}

	public MapDBDatabaseImpl setUseCache(boolean useCache) {
		this.useCache = useCache;
		return this;
	}

	public boolean isTemporary() {
		return isTemporary;
	}

	public MapDBDatabaseImpl setTemporary(boolean isTemporary) {
		this.isTemporary = isTemporary;
		return this;
	}

	public boolean isUseSingleLock() {
		return useSingleLock;
	}

	public MapDBDatabaseImpl setUseSingleLock(boolean useSingleLock) {
		this.useSingleLock = useSingleLock;
		return this;
	}

	public boolean isUseCompression() {
		return useCompression;
	}

	public MapDBDatabaseImpl setUseCompression(boolean useCompression) {
		this.useCompression = useCompression;
		return this;
	}

	public int getAsyncWriteQueueSize() {
		return asyncWriteQueueSize;
	}

	public MapDBDatabaseImpl setAsyncWriteQueueSize(int asyncWriteQueueSize) {
		this.asyncWriteQueueSize = asyncWriteQueueSize;
		return this;
	}
	
	public boolean isFileDb() {
		return isFileDB;
	}

	public MapDBDatabaseImpl setFileDb(boolean isFileDB) {
		this.isFileDB = isFileDB;
		return this;
	}

}
