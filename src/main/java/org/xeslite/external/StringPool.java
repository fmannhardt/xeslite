package org.xeslite.external;

public interface StringPool {

	Integer put(String val);

	Integer getIndex(String val);

	String getValue(int index);
	
	int size();
	
	int getCapacity();

}