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
     * Replaces old base timestamp for the operation with the new one.
     * 
     * @param ts
     */
    void replaceBaseTimestamp(Timestamp ts);

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

    void applyTo(V crdt);
}
