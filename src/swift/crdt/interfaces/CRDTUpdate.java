package swift.crdt.interfaces;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;

/**
 * Basic interface for representing an update operation on a CRDT
 * 
 * @author nmp, annettebieniusa
 */
public interface CRDTUpdate<V extends CRDT<V>> {

    /**
     * Returns the timestamp associated to the operations.
     */
    TripleTimestamp getTimestamp();

    /**
     * Returns a deep copy of this operation with another base timestamp.
     * 
     * @param ts
     *            base timestamp to use in the copy
     */
    CRDTUpdate<V> withBaseTimestamp(Timestamp ts);

    /**
     * Replaces base timestamp of dependee operation(s) of this operation with
     * the new one.
     * 
     * @param oldTs
     *            old base timestamp of a dependent operation
     * @param newTs
     *            new base timestamp of a dependent operation
     */
    void replaceDependeeOperationTimestamp(Timestamp oldTs, Timestamp newTs);

    /**
     * Applies operation to the given object instance.
     * 
     * @param crdt
     *            object where operation is applied
     */
    void applyTo(V crdt);
}
