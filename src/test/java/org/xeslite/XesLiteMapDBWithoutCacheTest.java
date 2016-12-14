package org.xeslite;

import static org.junit.Assert.assertEquals;

import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryRegistry;
import org.deckfour.xes.model.XLog;
import org.junit.Test;
import org.xeslite.external.XFactoryExternalStore;

public class XesLiteMapDBWithoutCacheTest extends XesLiteBaseTestAbstract {

	@Test
	public void testCreateReadRandomLogDiskWithoutCache() {
		XFactory factory = new XFactoryExternalStore.MapDBDiskWithoutCacheImpl();
		XFactoryRegistry.instance().setCurrentDefault(factory);
		XLog log = createRandomLog(factory, TEST_SIZE);
		assertEquals(TEST_SIZE, log.size());
		readSequentially(log);
		readSequentiallyCommon(log);
		readRandom(log);
		changeAttributes(log);
	}
	

}
