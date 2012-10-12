package swift.crdt.interfaces;

import swift.client.CommitListener;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

/**
 * Representation of transaction, a basic unit of application interaction with
 * the Swift system. All read of objects accessed through a transaction (
 * {@link #get(CRDTIdentifier, boolean, Class)}) ensure guarantees specified by
 * transaction {@link IsolationLevel}. The freshness of read objects is
 * determined by {@link CachePolicy}. All updates issued on objects within a
 * transaction become atomically visible to other transactions at some time
 * after commit ({@link #commit(boolean)}).
 * 
 * @author annettebieniusa
 * 
 */
// WISHME: separate client and system interface (needed for mocks)
public interface TxnHandle {
    /**
     * ObjectUpdatesListener that can be used in calls to
     * {@link #get(CRDTIdentifier, boolean, Class, ObjectUpdatesListener)},
     * forcing client to subscribe updates on an object. Note that application
     * requiring notification on updates can implement
     * {@link ObjectUpdatesListener} directly.
     */
    ObjectUpdatesListener UPDATES_SUBSCRIBER = new ObjectUpdatesListener() {
        @Override
        public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {
            // no-op
        }

        @Override
        public boolean isSubscriptionOnly() {
            return true;
        }
    };

    /**
     * See {@link #get(CRDTIdentifier, boolean, Class, ObjectUpdatesListener)}
     * with no updates listener
     */
    <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T get(CRDTIdentifier id, boolean create, Class<V> classOfT)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException;

    /**
     * Returns an object of the provided identifier. If object is not in the
     * store, it is created if create equals true (the creation takes effect
     * after the transaction commits), otherwise renders error.
     * <p>
     * This call may block if no appropriate version of an object is available
     * in the local cache of the client according to transaction's isolation
     * level and cache policy, as it requires communication with server in such
     * case.
     * <p>
     * When {@link ObjectUpdatesListener} is provided, client subscribes updates
     * on an object. Note that this subscription may expire if notification has
     * already been delivered and object has not been accessed recently by any
     * transaction running in the client.
     * 
     * @param id
     *            identifier of an object
     * @param create
     *            when true if object does not exist in the store, it is
     *            created; otherwise call fails; for read-only transactions, an
     *            initial state of an object is presented i the object is not in
     *            the store
     * @param classOfT
     *            class of an object stored (or created) under this identifier;
     *            it is the responsibility of application to ensure uniform
     *            id<->type mapping across the system; TODO: reconsider adding
     *            type as part of an id which would resolve this kind of issues
     * @param updatesListener
     *            listener that will receive a notification when a newer version
     *            of an object than the returned one is available in the store
     *            before the client reads this object again; note that this
     *            event may fire even during this call or after the transaction
     *            has committed; when null, notification is disabled; the
     *            provided listener will be fired at most once per each get call
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
     * @throws NetworkException
     *             when the state of local cache and/or cachePolicy requires
     *             communication with the store and connection fails; client may
     *             repeat the call or start a transaction with different
     *             settings
     */
    <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T get(CRDTIdentifier id, boolean create, Class<V> classOfT,
            final ObjectUpdatesListener updatesListener) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException;

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
    <V extends CRDT<V>> void registerOperation(final CRDTIdentifier id, CRDTUpdate<V> op);

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
