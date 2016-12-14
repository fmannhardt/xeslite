package org.xeslite;

import java.io.FileNotFoundException;

import org.deckfour.xes.factory.XFactoryBufferedImpl;
import org.junit.Test;
import org.xeslite.parser.XesLiteXmlParser;

public class LoadBufferedTestLocal extends LoadTestBase {

	@Test
	public void testLoadTestLog() throws FileNotFoundException, Exception {
		XFactoryBufferedImpl factory = new XFactoryBufferedImpl();
		doTest(new XesLiteXmlParser(factory, false), factory);			
	}
	
}
