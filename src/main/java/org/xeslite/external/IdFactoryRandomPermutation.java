package org.xeslite.external;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates unique, sequential IDs
 * 
 * @author F. Mannhardt
 * 
 */
final class IdFactoryRandomPermutation implements Serializable, IdFactory {

	private static final long serialVersionUID = 2472277562228753427L;

	private static final long PRIME = 4294967291l;
	private static final long HALFPRIME = PRIME / 2;

	private long seed = permuteQPR(new Random().nextInt() ^ 0x46790905);

	private final int intervalShift;
	private AtomicLong index;

	/**
	 * Creates a new ID factory.
	 */
	IdFactoryRandomPermutation(int intervalShift, Long initialValue) {
		this.intervalShift = intervalShift;
		if (initialValue != null) {
			this.index = new AtomicLong(initialValue / intervalShift);
		} else {
			this.index = new AtomicLong();
		}
	}

	private static long permuteQPR(long x) {
		if (x >= PRIME) {
			// Give up, ran out of numbers the following ones are just sequential
			return x;
		} else if (x <= HALFPRIME) {
			return quadraticResidue(x);
		} else {
			return PRIME - quadraticResidue(x);
		}
	}

	private static long quadraticResidue(long x) {
		return (x * x) % PRIME;
	}

	/**
	 * Creates a new, unique ID.
	 * 
	 * @return Unique ID.
	 */
	public synchronized long nextId() {
		long currentIndex = index.incrementAndGet();
		long permutatedIndex = permuteQPR(permuteQPR(currentIndex) + seed ^ 0x5bf03635l);
		return permutatedIndex << intervalShift;
	}

	public int getIntervalCapacity() {
		return 1 << intervalShift;
	}

	public int getIdShift() {
		return intervalShift;
	}

}