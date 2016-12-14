package org.xeslite;

import java.io.FileNotFoundException;

import org.junit.Test;
import org.xeslite.lite.factory.XFactoryLiteImpl;
import org.xeslite.parser.XesLiteXmlParser;

public class LoadLiteTestLocal extends LoadTestBase {
	
	@Test
	public void testLoadTestLog() throws FileNotFoundException, Exception {
		XFactoryLiteImpl factory = new XFactoryLiteImpl();
		doTest(new XesLiteXmlParser(factory, false), factory);			
	}
	
}
