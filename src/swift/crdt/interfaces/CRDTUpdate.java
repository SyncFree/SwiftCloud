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
     * Set timestamp mapping information for this update. Note that the provided
     * mapping should be a non-strict superset of the old one.
     * 
     * @param mapping
     *            timestamp mapping to install
     */
    void setTimestampMapping(TimestampMapping mapping);

    /**
     * Applies operation to the given object instance.
     * 
     * @param crdt
     *            object where operation is applied
     */
    void applyTo(V crdt);
}
