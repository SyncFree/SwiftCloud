package swift.crdt.interfaces;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;

/**
 * Basic interface for representing an operation in a CRDT
 * 
 * @author nmp, annettebieniusa
 */
public interface CRDTOperation<V extends CRDT<V>> {

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
    CRDTOperation<V> withBaseTimestamp(Timestamp ts);

    /**
     * Replaces base timestamp of depending operation(s) of this operation with
     * the new one.
     * 
     * @param oldTs
     *            old base timestamp of a dependent operation
     * @param newTs
     *            new base timestamp of a dependent operation
     */
    void replaceDependentOpTimestamp(Timestamp oldTs, Timestamp newTs);

    /**
     * Applies operation to the given object instance.
     * 
     * @param crdt
     *            object where operation is applied
     */
    void applyTo(V crdt);
}
