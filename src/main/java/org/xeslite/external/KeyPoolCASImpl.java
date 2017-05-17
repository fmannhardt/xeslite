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
public final class KeyPoolCASImpl implements Serializable, StringPool {

	private static final long serialVersionUID = 5412456627184375334L;

	private static final Integer INSERTING = new Integer(-1);

	private final AtomicInteger keyCounter;
	private final NonBlockingHashMap<String, Integer> keyMap;
	private final NonBlockingHashMapLong<String> valueMap;

	public KeyPoolCASImpl() {
		super();
		this.keyCounter = new AtomicInteger(3); // 0-2 reserved for common attributes
		this.keyMap = new NonBlockingHashMap<>();
		this.valueMap = new NonBlockingHashMapLong<>();
	}

	@Override
	public Integer put(String val) {

		if (val == null) {
			throw new XESLiteException("Key pool cannot store a 'NULL' value!");
		}

		switch (val) {
			case "concept:name" :
				return 0;
			case "time:timestamp" :
				return 1;
			case "lifecycle:transition" :
				return 2;
			default :
				return doGet(val);
		}
	}

	private Integer doGet(String val) {
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

	@Override
	public Integer getIndex(String val) {
		switch (val) {
			case "concept:name" :
				return 0;
			case "time:timestamp" :
				return 1;
			case "lifecycle:transition" :
				return 2;
			default :
				return doGetIndex(val);
		}		
	}

	private Integer doGetIndex(String val) {
		Integer index = keyMap.get(val);
		while (index != null && index == INSERTING) {
			LockSupport.parkNanos(10L);
			index = keyMap.get(val);
		}
		return index;
	}

	@Override
	public String getValue(int index) {
		switch (index) {
			case 0 :
				return "concept:name";
			case 1 :
				return "time:timestamp";
			case 2 :
				return "lifecycle:transition";
			default :
				return doGetValue(index);
		}			
	}

	private String doGetValue(int index) {
		return valueMap.get(index);
	}

	@Override
	public String toString() {
		return String.format("KeyPoolCASImpl with %s entries", keyCounter);
	}

	public int size() {
		return keyMap.size();
	}

	public int getCapacity() {
		return Integer.MAX_VALUE;
	}

}