package org.xeslite.external;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class KeyStringPoolTest {

	private static final class RunnableImpl implements Runnable {
		private final String[] keyPool;
		private final StringPool pool;
		private final int keyIdx;

		private RunnableImpl(String[] keyPool, StringPool pool, int keyIdx) {
			this.keyPool = keyPool;
			this.pool = pool;
			this.keyIdx = keyIdx;
		}

		public void run() {
			String key = keyPool[keyIdx];			
			Integer index = pool.put(key);
			assertEquals(key, pool.getValue(index));
			Integer index2 = pool.getIndex(key);				
			assertEquals(index, index2);
			assertEquals(index, pool.put(key));
		}

	}

	@Test
	public void testKeyPoolCAS() {
		StringPoolCASImpl pool = new StringPoolCASImpl(16384);
		testIntegrity(pool);		
		testPool(casPoolCallable);
	}
	
	private void testIntegrity(final StringPool pool) {
		Integer test1 = pool.put("Test1");
		Integer test2 = pool.put("Test2");
		Integer test3 = pool.put("Test1");
		assertEquals(test1, test3);
		assertEquals(test1, pool.getIndex("Test1"));
		assertEquals(test2, pool.getIndex("Test2"));
		assertEquals(test3, pool.getIndex("Test1"));
	}

	private void testPool(Collection<? extends Runnable> actions) {		
		ExecutorService threadPool = Executors.newFixedThreadPool(8);
		for (Runnable r: actions) {
			threadPool.submit(r);
		}
		threadPool.shutdown();
		try {
			threadPool.awaitTermination(100, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private static List<Runnable> casPoolCallable = prepareCallables(new StringPoolCASImpl(16384), 5000000);

	private static List<Runnable> prepareCallables(final StringPool pool, int limit) {
		final Random random = new Random();
		final int COUNT = pool.getCapacity();
		final String[] keyPool = new String[COUNT];	
		for (int i = 0; i < COUNT; i++) {
			keyPool[i] = "Key"+i;
		}
		Runnable callable = new RunnableImpl(keyPool, pool, random.nextInt(COUNT));
		List<Runnable> actions = new ArrayList<Runnable>();	
		for (int k = 0; k < limit; k++) {
			actions.add(callable);
		}
		return actions;
	}

}
