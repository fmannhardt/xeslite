package org.xeslite;

import org.junit.Test;
import org.xeslite.external.XFactoryExternalStore;
import org.xeslite.external.XFactoryExternalStore.MapDBDiskSequentialAccessImpl;

public class ShutdownHookTest {

	@Test
	public void testShutdown() {
		for (int i = 0; i < 11; i++) {
			MapDBDiskSequentialAccessImpl factory = new XFactoryExternalStore.MapDBDiskSequentialAccessImpl();
			factory.dispose();
		}
		for (int i = 0; i < 11; i++) {
			new XFactoryExternalStore.MapDBDiskSequentialAccessImpl();
		}
	}

	public static void main(String[] args) throws InterruptedException {
		new ShutdownHookTest().testShutdown();
		Thread.sleep(2000);
	}

}
