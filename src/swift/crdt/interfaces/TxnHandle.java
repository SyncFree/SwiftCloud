package swift.crdt.interfaces;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

/**
 * Interface for transaction handles.
 * 
 * @author annettebieniusa
 * 
 */
public interface TxnHandle {
    // TODO specify fail mode/timeout for get() - if we support disconnected
    // operations, it cannot be that a synchronous call fits everything.
    /**
     * Returns an object of the provided identifier. If object is not in the
     * store, the
     * 
     * @param id
     * @param create
     * @param classOfT
     * @return
     * @throws WrongTypeException
     * @throws NoSuchObjectException
     */
    <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T get(CRDTIdentifier id, boolean create, Class<V> classOfT)
            throws WrongTypeException, NoSuchObjectException, ConsistentSnapshotVersionNotFoundException;

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
     * Registers a new CRDT operation on an object in this transaction. Only
     * called by system (CRDT) object.
     * 
     * @param id
     *            object identifier
     * @param op
     *            operation
     */
    void registerOperation(final CRDTIdentifier id, CRDTOperation<?> op);
}
