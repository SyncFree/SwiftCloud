package swift.crdt.interfaces;

import swift.client.CommitListener;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

/**
 * Representation of transaction, a basic unit of application interaction with
 * the Swift system. All read of objects accessed through a transaction (
 * {@link #get(CRDTIdentifier, boolean, Class)}) constitute some consistent
 * snapshot of the system. All updates issued on these objects within a
 * transaction become atomically visible to other transactions at some time
 * after commit ({@link #commit(boolean)}).
 * 
 * @author annettebieniusa
 * 
 */
// TODO: BTW of testing, separate client and system interface (needed for mocks)
public interface TxnHandle {
    /**
     * See {@link #get(CRDTIdentifier, boolean, Class, ObjectUpdatesListener)}
     * with no updates listener
     */
    <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T get(CRDTIdentifier id, boolean create, Class<V> classOfT)
            throws WrongTypeException, NoSuchObjectException, ConsistentSnapshotVersionNotFoundException;

    /**
     * Returns an object of the provided identifier. If object is not in the
     * store, it is created if create equals true (the creation takes effect
     * after the transaction commits), otherwise renders error.
     * <p>
     * This call may block if no appropriate (consistent) version of an object
     * is available in the local cache of the client, as it requires
     * communication with server in such case.
     * 
     * TODO specify fail mode/timeout for get() - if we support disconnected
     * operations, it cannot be that a synchronous call fits everything.
     * 
     * @param id
     *            identifier of an object
     * @param create
     *            when true if object does not exist in the store, it is
     *            created; otherwise call fails
     * @param classOfT
     *            class of an object stored (or created) under this identifier;
     *            it is the responsibility of application to ensure uniform
     *            id<->type mapping across the system; TODO: reconsider adding
     *            type as part of an id which would resolve this kind of issues
     * @param updatesListener
     *            listener that will receive a notification when a newer version
     *            of an object than the returned one is available in the store;
     *            note that this event may fire even during this call; when
     *            null, notification is disabled
     * @return transactional view of an object; accepts queries and updates;
     *         note that this view of an object is valid only until the
     *         transaction is committed or rolled back
     * @throws WrongTypeException
     *             when classOfT does not match the type of object stored under
     *             identifier id
     * @throws NoSuchObjectException
     *             when create is false and object does not exist in the store
     * @throws IllegalStateException
     *             when transaction is already committed or rolled back
     */
    <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T get(CRDTIdentifier id, boolean create, Class<V> classOfT,
            final ObjectUpdatesListener updatesListener) throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException;

    /**
     * Commits the transaction and blocks until the transaction is committed to
     * the store.
     * 
     * @throws IllegalStateException
     *             when transaction is already committed or rolled back
     */
    void commit();

    /**
     * Commits the transaction, blocks only until local commit and commits to
     * the store asynchronously.
     * 
     * @param listener
     *            listener notified about transaction global commit;
     *            notification may be called by a system thread and should
     *            minimize the processing; ignored if null
     * @throws IllegalStateException
     *             when transaction is already committed or rolled back
     */
    void commitAsync(final CommitListener listener);

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
