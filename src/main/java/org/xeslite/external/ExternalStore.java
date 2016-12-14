package org.xeslite.external;

import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XLog;

/**
 * A store that stores {@link ExternalIdentifyable} objects.
 * 
 * @author F. Mannhardt
 * 
 */
public interface ExternalStore {

	XAttributeMap getAttributes(ExternalAttributable attributable);

	void setAttributes(ExternalAttributable attributable, XAttributeMap attributes);

	XAttributeMap removeAttributes(ExternalAttributable attributable);

	boolean hasAttributes(ExternalAttributable attributable);

	/**
	 * @return a {@link StringPool} that is used to map key {@link String}s to
	 *         {@link Integer}s
	 */
	StringPool getAttributeKeyPool();

	/**
	 * @return a {@link StringPool} that is used to map general literals
	 *         {@link String}s to {@link Integer}s
	 */
	StringPool getLiteralPool();

	/**
	 * @return a {@link IdFactory} that can be used to obtain new 'unique'
	 *         identifiers for objects stored in the {@link ExternalStore}
	 */
	IdFactory getIdFactory();

	/**
	 * Commits all pending changes to the underlying storage
	 */
	void commit();

	/**
	 * Closes the storage and releases all resources
	 */
	void dispose();

	/**
	 * Starts the batch import mode (can only be done once). Be aware that this
	 * may lock the underlying storage. So only the returned {@link PumpService}
	 * MUST be used. Any other operation will fail or lead to a deadlock!
	 * 
	 * @return
	 */
	PumpService startPump();

	/**
	 * @return the current batch importer
	 */
	PumpService getPumpService();

	/**
	 * @return whether the batch import mode is active
	 */
	boolean isPumping();

	/**
	 * Saves the XLog to the external store
	 * 
	 * @param log
	 */
	void saveLogStructure(XLog log);

	/**
	 * Loads a saved XLog from the external store
	 * 
	 * @param factory
	 * @return
	 */
	XLog loadLogStructure(XFactoryExternalStore factory);

}