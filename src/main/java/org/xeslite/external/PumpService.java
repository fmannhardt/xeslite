package org.xeslite.external;

import java.util.List;

import org.deckfour.xes.model.XAttributable;
import org.deckfour.xes.model.XAttribute;

/**
 * Batch import service
 * 
 * @author F. Mannhardt
 *
 */
interface PumpService {

	void pumpAttributes(XAttributable attributable, List<XAttribute> collection);

	void finishPump() throws InterruptedException;
}
