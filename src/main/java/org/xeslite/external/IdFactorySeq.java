package org.xeslite.external;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates unique, sequential IDs
 * 
 * @author F. Mannhardt
 * 
 */
public final class IdFactorySeq implements Serializable, IdFactory {

	private static final long serialVersionUID = -6646401822903367775L;

	private final int intervalShift;
	private AtomicLong counter;

	IdFactorySeq(int intervalShift) {
		this(intervalShift, null);
	}

	IdFactorySeq(int intervalShift, Long initialValue) {
		this.intervalShift = intervalShift;
		if (initialValue != null) {
			this.counter = new AtomicLong(initialValue >> intervalShift);
		} else {
			this.counter = new AtomicLong();
		}
	}

	/**
	 * Creates a new, unique ID.
	 * 
	 * @return Unique ID.
	 */
	public long nextId() {
		return counter.getAndIncrement() << intervalShift;
	}

	public int getIntervalCapacity() {
		return 1 << intervalShift;
	}

	public int getIdShift() {
		return intervalShift;
	}

}