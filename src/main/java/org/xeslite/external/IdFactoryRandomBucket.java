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
final class IdFactoryRandomBucket implements Serializable, IdFactory {

	private static final long serialVersionUID = 9153690446521951367L;
	
	private static final int BUFFER_SIZE = 16 * 1024;

	private final int intervalShift;

	private long counter = 0;
	private int bufferCounter = BUFFER_SIZE;

	private long[] nextNumbers = new long[BUFFER_SIZE];

	/**
	 * Creates a new ID factory with an initial counter of 0.
	 * 
	 * @param intervalShift
	 */
	IdFactoryRandomBucket(int intervalShift) {
		this(intervalShift, null);
	}

	/**
	 * Creates a new ID factory with an initial counter value.
	 * 
	 * @param intervalShift
	 * @param initialValue
	 */
	IdFactoryRandomBucket(int intervalShift, Long initialValue) {
		this.intervalShift = intervalShift;
		if (initialValue != null) {
			this.counter = initialValue >> intervalShift;
		}
	}

	private void updateBuffer() {
		for (int i = 0; i < BUFFER_SIZE; i++) {
			counter++;
			nextNumbers[i] = counter;
		}
		Collections.shuffle(Longs.asList(nextNumbers));
		bufferCounter = 0;
	}

	/**
	 * Creates a new, unique ID.
	 * 
	 * @return Unique ID.
	 */
	public synchronized long nextId() {
		bufferCounter++;
		if (bufferCounter >= BUFFER_SIZE) {
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