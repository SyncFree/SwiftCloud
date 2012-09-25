package swift.crdt.interfaces;

import swift.clocks.Timestamp;
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

    void setTimestampMapping(TimestampMapping mapping);
    
    /**
     * Adds a system timestamp to this update.
     * 
     * @param ts
     *            new target system timestamp
     */
    void addSystemTimestamp(Timestamp ts);

    /**
     * Applies operation to the given object instance.
     * 
     * @param crdt
     *            object where operation is applied
     */
    void applyTo(V crdt);
}
