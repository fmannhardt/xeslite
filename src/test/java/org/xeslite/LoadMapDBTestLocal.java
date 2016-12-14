package org.xeslite;

import java.io.FileNotFoundException;

import org.junit.Test;
import org.xeslite.external.XFactoryExternalStore;
import org.xeslite.parser.XesLiteXmlParser;

public class LoadMapDBTestLocal extends LoadTestBase {
	
	@Test
	public void testLoadTestLog() throws FileNotFoundException, Exception {
		XFactoryExternalStore.MapDBDiskImpl factory = new XFactoryExternalStore.MapDBDiskImpl();
		doTest(new XesLiteXmlParser(factory, false), factory);			
	}
	
	@Test
	public void testLoadTestLogLowMemory() throws FileNotFoundException, Exception {
		XFactoryExternalStore.MapDBDiskSequentialAccessImpl factory = new XFactoryExternalStore.MapDBDiskSequentialAccessImpl();
		doTest(new XesLiteXmlParser(factory, false), factory);			
	}

}

