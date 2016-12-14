package org.xeslite.external;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.xeslite.common.XESLiteException;

/**
 * @author F. Mannhardt
 * 
 */
public final class StringPoolCASImpl implements Serializable, StringPool {

	private static final long serialVersionUID = -2582640172305237779L;

	private static final Integer INSERTING = new Integer(-1);

	private final AtomicInteger keyCounter;
	private final NonBlockingHashMap<String, Integer> keyMap;
	private final NonBlockingHashMapLong<String> valueMap;

	// an optional fixed capacity
	private final int capacity;

	public StringPoolCASImpl() {
		this(Integer.MAX_VALUE);
	}

	public StringPoolCASImpl(int capacity) {
		super();
		this.capacity = capacity;
		this.keyCounter = new AtomicInteger(0);
		this.keyMap = new NonBlockingHashMap<>();
		this.valueMap = new NonBlockingHashMapLong<>();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.progressmining.xeslite.external.StringPool#put(java.lang.String)
	 */
	@Override
	public Integer put(String val) {

		if (keyCounter.get() >= capacity) {
			throw new XESLiteException.StringPoolException(
					"Too many distinct literals to be stored in this string pool. The maximum number of literals that can be stored is "
							+ capacity);
		}

		if (val == null) {
			throw new XESLiteException("String pool cannot store a 'NULL' value!");
		}

		// First check for existence, as we expect the same String to be used multiple times
		Integer index = keyMap.get(val);

		if (index != null) {
			while (index == INSERTING) {
				LockSupport.parkNanos(10L);
				index = keyMap.get(val);
			}
			return index;
		} else {
			// No yet present, insert a stub value as we don't want to waste indices
			index = keyMap.putIfAbsent(val, INSERTING);
			if (index == null) {
				// We reserved the spot and, therefore, the following operations are executed atomically
				// Take care that this block never fails, otherwise other threads trying the same put starve
				index = keyCounter.getAndIncrement();
				valueMap.put(index, val);
				keyMap.put(val, index);
				return index;
			} else {
				// Another thread reserved the sport, wait until insert is visible to us
				while (index == INSERTING) {
					LockSupport.parkNanos(10L);
					index = keyMap.get(val);
				}
				return index;
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.progressmining.xeslite.external.StringPool#getIndex(java.lang.String)
	 */
	@Override
	public Integer getIndex(String val) {
		Integer index = keyMap.get(val);
		while (index != null && index == INSERTING) {
			LockSupport.parkNanos(10L);
			index = keyMap.get(val);
		}
		return index;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.progressmining.xeslite.external.StringPool#getValue(java.lang.Integer
	 * )
	 */
	@Override
	public String getValue(int index) {
		return valueMap.get(index);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("StringPoolCASImpl with %s entries", keyCounter);
	}

	public int getCapacity() {
		return capacity;
	}

	public int size() {
		return keyMap.size();
	}

}