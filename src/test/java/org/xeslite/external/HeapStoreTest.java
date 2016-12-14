package org.xeslite.external;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.xeslite.external.InMemoryStore.HeapVolume;
import org.xeslite.external.InMemoryStore.Mode;

public class HeapStoreTest {

	private final class RunnableImpl implements Runnable {

		private final HeapVolume storage;

		public RunnableImpl(HeapVolume storage) {
			this.storage = storage;
		}

		public void run() {
			final Random random = new Random();
			for (int i = 0; i < 1_000_000; i++) {
				int position = random.nextInt(1 << 20);
				long value = random.nextLong();
				long nested = random.nextBoolean() ? 1 : -1;
				storage.putLong(position, value, nested);
				assert storage.hasValue(position);
			}
		}

	}

	@Test
	public void testConcurrentHeapStore() {
		InMemoryStore.HeapVolume storage = new InMemoryStore.HeapVolume(4, 11, Mode.LONG);

		ExecutorService threadPool = Executors.newFixedThreadPool(8);
		for (int i = 0; i < 100; i++) {
			threadPool.submit(new RunnableImpl(storage));
		}
		threadPool.shutdown();
		try {
			threadPool.awaitTermination(100, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testBooleanHeapStore() {
		InMemoryStore.HeapVolume storage = new InMemoryStore.HeapVolume(4, 11, Mode.BOOLEAN);
		final Random random = new Random();
		for (int i = 0; i < 10000; i++) {
			boolean val = random.nextBoolean();
			int pos = random.nextInt(10000);
			long nested = random.nextBoolean() ? 1 : -1;
			storage.putBoolean(pos, val, nested);
			storage.compressStorage();
			assertTrue(storage.hasValue(pos));
			assertEquals(val, storage.getBoolean(pos));
			assertTrue((nested != -1) ? storage.hasNested(pos) : !storage.hasNested(pos));
		}
	}

	@Test
	public void testLongHeapStore() {
		InMemoryStore.HeapVolume storage = new InMemoryStore.HeapVolume(4, 11, Mode.LONG);
		final Random random = new Random();
		for (int i = 0; i < 10000; i++) {
			long val = random.nextLong();
			int pos = random.nextInt(10000);
			long nested = random.nextBoolean() ? 1 : -1;
			storage.putLong(pos, val, nested);
			storage.compressStorage();
			assertTrue(storage.hasValue(pos));
			assertEquals(val, storage.getLong(pos));
			assertTrue((nested != -1) ? storage.hasNested(pos) : !storage.hasNested(pos));
		}
	}

	@Test
	public void testIntHeapStore() {
		InMemoryStore.HeapVolume storage = new InMemoryStore.HeapVolume(4, 11, Mode.INT);
		final Random random = new Random();
		for (int i = 0; i < 10000; i++) {
			int val = random.nextInt();
			int pos = random.nextInt(10000);
			long nested = random.nextBoolean() ? 1 : -1;
			storage.putInt(pos, val, nested);
			storage.compressStorage();
			assertTrue(storage.hasValue(pos));
			assertEquals(val, storage.getInt(pos));
			assertTrue((nested != -1) ? storage.hasNested(pos) : !storage.hasNested(pos));
		}
	}

}
