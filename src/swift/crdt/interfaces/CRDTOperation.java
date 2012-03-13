package swift.crdt.interfaces;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;

/**
 * Basic interface for representing an operation in a CRDT
 * 
 * @author nmp, annettebieniusa
 */
// TODO: in presence of CRDTObjectOperationsGroup, we may need to rethink
// (simplify?) this interface and interaction with CRDT definition and TxnHandle
public interface CRDTOperation {

    /**
     * Get the identifier of CRDT on which operation is to be executed.
     * 
     * @return CRDT identifier on which operation is to be executed
     */
    CRDTIdentifier getTargetUID();

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
     * Returns the causality clock for the objects on which the operation is to
     * be executed.
     * 
     * @return causality clock for object state on which operation is executed
     */
    CausalityClock getDependency();
}
