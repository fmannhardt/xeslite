package org.xeslite.external;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public final class IdFactorySeqDecreasing implements Serializable, IdFactory {

	private static final long serialVersionUID = 3619626124089675199L;

	private final int intervalShift;
	private AtomicLong counter;

	/**
	 * Creates a new ID factory.
	 */
	IdFactorySeqDecreasing(int intervalShift, Long initialValue) {
		this.intervalShift = intervalShift;
		if (initialValue != null) {
			this.counter = new AtomicLong(initialValue >> intervalShift);
		} else {
			this.counter = new AtomicLong(Long.MAX_VALUE >> intervalShift);
		}
	}

	IdFactorySeqDecreasing(int intervalShift) {
		this(intervalShift, null);
	}

	/**
	 * Creates a new, unique ID.
	 * 
	 * @return Unique ID.
	 */
	public long nextId() {
		return counter.decrementAndGet() << intervalShift;
	}

	public int getIntervalCapacity() {
		return 1 << intervalShift;
	}

	public int getIdShift() {
		return intervalShift;
	}

}
