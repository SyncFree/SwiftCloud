package swift.crdt.interfaces;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;

/**
 * Basic interface for representing an operation in a CRDT
 * 
 * @author nmp, annettebieniusa
 */
public interface CRDTOperation {

    /**
     * Returns the timestamp associated to the operations.
     */
    TripleTimestamp getTimestamp();

    /**
     * Replaces old base timestamp for the operation with the new one.
     * 
     * @param ts
     */
    void replaceBaseTimestamp(Timestamp ts);

}
