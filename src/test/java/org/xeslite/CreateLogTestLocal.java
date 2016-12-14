package org.xeslite;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryRegistry;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.out.XesXmlGZIPSerializer;
import org.junit.Test;
import org.xeslite.lite.factory.XFactoryLiteImpl;

public class CreateLogTestLocal extends XesLiteBaseTestAbstract {
	
	@Test
	public void testCreateReadRandomLogBuffered() {
		XFactory factory = new XFactoryLiteImpl();
		XFactoryRegistry.instance().setCurrentDefault(factory);
		XLog log = createRandomLog(factory, 50000);
		XesXmlGZIPSerializer serializer = new XesXmlGZIPSerializer();
		try {
			serializer.serialize(log, new BufferedOutputStream(new FileOutputStream("testlog.xes.gz", false)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
