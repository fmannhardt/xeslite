package org.xeslite;

import java.io.FileNotFoundException;

import org.junit.Test;
import org.xeslite.external.XFactoryExternalStore;

public class EntrySetMapDBTestLocal extends EntrySetBaseTestAbstract {
	
	@Test
	public void testLoadTestLog() throws FileNotFoundException, Exception {
		doTest(new XFactoryExternalStore.MapDBDiskImpl());			
	}
	
	@Test
	public void testLoadTestLogSequential() throws FileNotFoundException, Exception {
		doTest(new XFactoryExternalStore.MapDBDiskSequentialAccessImpl());			
	}
	
}
