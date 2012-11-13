package swift.crdt.interfaces;

import swift.clocks.TimestampMapping;
import swift.clocks.TripleTimestamp;

/**
 * Basic interface for representing an update operation on a CRDT
 * 
 * @author nmp, annettebieniusa
 */
public interface CRDTUpdate<V extends CRDT<V>> {

    /**
     * Returns the id associated to the operations.
     */
    TripleTimestamp getTimestamp();

    /**
     * Applies operation to the given object instance.
     * 
     * @param crdt
     *            object where operation is applied
     */
    void applyTo(V crdt);

    // Dirty tricks: sharing and unsharing a timestamp mapping instance to save
    // space and message size... and avoid troubles.
    /**
     * Sets new timestamp mapping for a timestamp
     * 
     * @param mapping
     *            a timestamp mapping reference, compatible with the orginal one
     */
    void setTimestampMapping(TimestampMapping mapping);
}
