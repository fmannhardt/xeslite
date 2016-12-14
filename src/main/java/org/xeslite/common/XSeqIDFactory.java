package org.xeslite.common;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates unique, sequential IDs
 * 
 * @author F. Mannhardt
 * 
 */
public final class XSeqIDFactory {

	private static XSeqIDFactory singleton = new XSeqIDFactory();

	public static XSeqIDFactory instance() {
		return singleton;
	}

	private AtomicLong counter;

	/**
	 * Creates a new ID factory (hidden constructor).
	 */
	private XSeqIDFactory() {
		this.counter = new AtomicLong();
	}

	/**
	 * Creates a new, unique ID.
	 * 
	 * @return Unique ID.
	 */
	public long nextId() {		
		return counter.incrementAndGet();
	}

}