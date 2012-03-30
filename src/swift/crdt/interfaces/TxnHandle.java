package swift.crdt.interfaces;

import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

/**
 * Representation of transaction, a basic unit of application interaction with
 * the Swift system. All read of objects accessed through a transaction (
 * {@link #get(CRDTIdentifier, boolean, Class)}) constitute some consistent
 * snapshot of the system. All updates issued on these objects within
 * transaction become atomically visible to other transactions after commit (
 * {@link #commit(boolean)}).
 * 
 * @author annettebieniusa
 * 
 */
public interface TxnHandle {
    /**
     * Returns an object of the provided identifier. If object is not in the
     * store, the
     * <p>
     * This call may block if no appropriate (consistent) version of an object
     * is available in the local cache of the client, as it requires
     * communication with server in thatcase.
     * 
     * TODO specify fail mode/timeout for get() - if we support disconnected
     * operations, it cannot be that a synchronous call fits everything.
     * 
     * @param id
     * @param create
     * @param classOfT
     * @return
     * @throws WrongTypeException
     * @throws NoSuchObjectException
     * @throws IllegalStateException
     *             when transaction is already committed or rolled back
     */
    <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T get(CRDTIdentifier id, boolean create, Class<V> classOfT)
            throws WrongTypeException, NoSuchObjectException, ConsistentSnapshotVersionNotFoundException;

    /**
     * Commits the transaction.
     * 
     * TODO: add notification mechanism for async. global commit
     * 
     * @param waitForGlobalCommit
     *            when true, blocks until the transaction reaches the store;
     *            when false, awaits only local commit and commits at the store
     *            asynchronously
     * @throws IllegalStateException
     *             when transaction is already committed or rolled back
     */
    void commit(boolean waitForGlobalCommit);

    /**
     * Abandons the transaction and reverts any updates that were executed under
     * this transaction.
     * 
     * @throws IllegalStateException
     *             when transaction is already committed or rolled back
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
     * Registers a new CRDT operation on an object in this transaction. Called
     * only called by system (CRDT) object.
     * 
     * @param id
     *            object identifier
     * @param op
     *            operation
     */
    <V extends CRDT<V>> void registerOperation(final CRDTIdentifier id, CRDTOperation<V> op);

    /**
     * Registers a creation of CRDT object with a given initial empty state,
     * identified by the specified id. Called only called by system (CRDT)
     * object.
     * <p>
     * Creation can be registered before any other operation is registered and
     * can be done only once in a transaction.
     * 
     * @param id
     *            object identifier
     * @param creationState
     *            initial empty state of an object
     */
    <V extends CRDT<V>> void registerObjectCreation(final CRDTIdentifier id, V creationState);
}
