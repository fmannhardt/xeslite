package org.xeslite.external;

/**
 * Provides IDs for use in an {@link ExternalIdentifyable}.
 * 
 * @author F. Mannhardt
 *
 */
public interface IdFactory {

	long nextId();

	int getIntervalCapacity();

	int getIdShift();

}