package org.xeslite;

import static org.junit.Assert.assertEquals;

import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryRegistry;
import org.deckfour.xes.model.XLog;
import org.junit.Test;
import org.xeslite.lite.factory.XFactoryLiteImpl;

public class XesLiteLiteTest extends XesLiteBaseTestAbstract {
	
	@Test
	public void testCreateReadRandomLogLite() {
		XFactory factory = new XFactoryLiteImpl();
		XFactoryRegistry.instance().setCurrentDefault(factory);
		XLog log = createRandomLog(factory, TEST_SIZE);
		assertEquals(TEST_SIZE, log.size());
		readSequentially(log);
		readSequentiallyCommon(log);
		readRandom(log);
	}

}
