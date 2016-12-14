package org.xeslite.external;

import java.io.Serializable;
import java.util.Collections;

import com.google.common.primitives.Longs;

/**
 * Generates unique, pseudo-random IDs using a buffer of pre-generated numbers
 * that are shuffled.
 * 
 * @author F. Mannhardt
 * 
 */
final class IdFactoryRandomBucketDecreasing implements Serializable, IdFactory {

	private static final long serialVersionUID = -6054036718172370267L;

	private static final int BUFFER_SIZE = 16 * 1024;

	private final int intervalShift;

	private long counter;
	private int bufferCounter = 0;

	private long[] nextNumbers = new long[BUFFER_SIZE];

	/**
	 * Creates a new ID factory.
	 */
	IdFactoryRandomBucketDecreasing(int intervalShift, Long initialValue) {
		this.intervalShift = intervalShift;
		if (initialValue != null) {
			this.counter = initialValue >> intervalShift;
		} else {
			this.counter = Long.MAX_VALUE >> intervalShift;
		}
	}

	private void updateBuffer() {
		for (int i = 0; i < BUFFER_SIZE; i++) {
			counter--;
			nextNumbers[i] = counter;
		}
		Collections.shuffle(Longs.asList(nextNumbers));
		bufferCounter = BUFFER_SIZE;
	}

	/**
	 * Creates a new, unique ID.
	 * 
	 * @return Unique ID.
	 */
	public synchronized long nextId() {
		bufferCounter--;
		if (bufferCounter >= 0) {
			updateBuffer();
			return nextNumbers[bufferCounter] << intervalShift;
		} else {
			return nextNumbers[bufferCounter] << intervalShift;
		}
	}

	public int getIntervalCapacity() {
		return 1 << intervalShift;
	}

	public int getIdShift() {
		return intervalShift;
	}

}