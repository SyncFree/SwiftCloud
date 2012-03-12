package swift.crdt.interfaces;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;

/**
 * Interface for transaction handles.
 * 
 * @author annettebieniusa
 * 
 */
public interface TxnHandle {
    <V extends CRDT<V, I>, I extends CRDTOperation> V get(CRDTIdentifier id, boolean create, Class<V> classOfT);

    /**
     * Commits the transaction.
     */
    void commit();

    /**
     * Abandons the transaction and reverts any updates that were executed under
     * this transaction.
     */
    void rollback();

    /**
     * @return transaction status
     */
    TxnStatus getStatus();

    /**
     * Generates timestamps for operations. Only called by system.
     * 
     * @return next timestamp
     */
    TripleTimestamp nextTimestamp();

    /**
     * Returns the causality clock associated to this transaction handle. Only
     * called by system.
     * 
     * @return
     */
    CausalityClock getSnapshotClock();

    /**
     * Register a new CRDT operation with the transaction. Only called by
     * system.
     * 
     * @param op
     */
    <I extends CRDTOperation> void registerOperation(I op);
}
