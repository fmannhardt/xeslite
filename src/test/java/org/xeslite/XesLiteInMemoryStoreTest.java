package org.xeslite;

import static org.junit.Assert.assertEquals;

import org.deckfour.xes.factory.XFactoryRegistry;
import org.deckfour.xes.model.XLog;
import org.junit.Test;
import org.xeslite.external.XFactoryExternalStore;
import org.xeslite.external.XFactoryExternalStore.InMemoryStoreImpl;

public class XesLiteInMemoryStoreTest extends XesLiteBaseTestAbstract {
	
	@Test
	public void testCreateReadRandomLogInMemoryStore() {
		InMemoryStoreImpl factory = new XFactoryExternalStore.InMemoryStoreImpl();
		XFactoryRegistry.instance().setCurrentDefault(factory);
		XLog log = createRandomLog(factory, TEST_SIZE);		
		assertEquals(TEST_SIZE, log.size());
		factory.commit();
		readSequentially(log);
		readSequentiallyCommon(log);
		readRandom(log);
		changeAttributes(log);
	}

	@Test
	public void testCreateReadRandomLogInMemoryStoreForAlignment() {
		XFactoryExternalStore factory = new XFactoryExternalStore.InMemoryStoreAlignmentAwareImpl();
		XFactoryRegistry.instance().setCurrentDefault(factory);
		XLog log = createRandomLog(factory, TEST_SIZE);		
		assertEquals(TEST_SIZE, log.size());
		factory.commit();
		readSequentially(log);
		readSequentiallyCommon(log);
		readRandom(log);
		changeAttributes(log);
	}


}
