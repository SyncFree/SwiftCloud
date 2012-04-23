package swift.client;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.ObjectUpdatesListener;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.interfaces.TxnStatus;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

/**
 * FXIME: Update DOCS!!!
 * 
 * @author mzawirski
 */
abstract class AbstractTxnHandle implements TxnHandle {

    protected final TxnManager manager;
    protected CachePolicy cachePolicy;
    protected final Timestamp localTimestamp;
    protected final CausalityClock updatesDependencyClock;
    protected final IncrementalTripleTimestampGenerator timestampSource;
    protected Timestamp globalTimestamp;
    protected final Map<CRDTIdentifier, CRDTObjectOperationsGroup<?>> localObjectOperations;
    protected TxnStatus status;
    protected CommitListener commitListener;

    /**
     * @param manager
     *            manager maintaining this transaction
     * @param cachePolicy
     *            cache policy used by this transaction
     * @param localTimestamp
     *            local timestamp used for local operations of this transaction
     */
    AbstractTxnHandle(final TxnManager manager, final CachePolicy cachePolicy, final Timestamp localTimestamp) {
        this.manager = manager;
        this.cachePolicy = cachePolicy;
        this.localTimestamp = localTimestamp;
        this.updatesDependencyClock = ClockFactory.newClock();
        this.timestampSource = new IncrementalTripleTimestampGenerator(localTimestamp);
        this.localObjectOperations = new HashMap<CRDTIdentifier, CRDTObjectOperationsGroup<?>>();
        this.status = TxnStatus.PENDING;
    }

    @Override
    public synchronized <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T get(CRDTIdentifier id, boolean create,
            Class<V> classOfV) throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException {
        return get(id, create, classOfV, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T get(CRDTIdentifier id, boolean create,
            Class<V> classOfV, ObjectUpdatesListener listener) throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException {
        // TODO: implement listener support - client-side notifications
        assertStatus(TxnStatus.PENDING);
        try {
            return getImpl(id, create, classOfV);
        } catch (ClassCastException x) {
            throw new WrongTypeException(x.getMessage());
        }
    }

    protected abstract <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T getImpl(CRDTIdentifier id, boolean create,
            Class<V> classOfV) throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException;

    @Override
    public void commit() {
        final Semaphore commitSem = new Semaphore(0);
        commitAsync(new CommitListener() {
            @Override
            public void onGlobalCommit(TxnHandle transaction) {
                commitSem.release();
            }
        });
        commitSem.acquireUninterruptibly();
    }

    @Override
    public synchronized void commitAsync(final CommitListener listener) {
        assertStatus(TxnStatus.PENDING);
        this.commitListener = listener;
        manager.commitTxn(this);
    }

    protected void updateUpdatesDependencyClock(final CausalityClock clock) {
        assertStatus(TxnStatus.PENDING);
        updatesDependencyClock.merge(clock);
    }

    @Override
    public synchronized void rollback() {
        assertStatus(TxnStatus.PENDING);
        manager.discardTxn(this);
        status = TxnStatus.CANCELLED;
    }

    public synchronized TxnStatus getStatus() {
        return status;
    }

    @Override
    public synchronized TripleTimestamp nextTimestamp() {
        assertStatus(TxnStatus.PENDING);
        return timestampSource.generateNew();
    }

    @Override
    public synchronized <V extends CRDT<V>> void registerOperation(CRDTIdentifier id, CRDTOperation<V> op) {
        assertStatus(TxnStatus.PENDING);

        @SuppressWarnings("unchecked")
        CRDTObjectOperationsGroup<V> operationsGroup = (CRDTObjectOperationsGroup<V>) localObjectOperations.get(id);
        if (operationsGroup == null) {
            operationsGroup = new CRDTObjectOperationsGroup<V>(id, getLocalTimestamp(), null);
            localObjectOperations.put(id, operationsGroup);
        }
        operationsGroup.append(op);
    }

    @Override
    public synchronized <V extends CRDT<V>> void registerObjectCreation(CRDTIdentifier id, V creationState) {
        final CRDTObjectOperationsGroup<V> operationsGroup = new CRDTObjectOperationsGroup<V>(id, getLocalTimestamp(),
                creationState);
        if (localObjectOperations.put(id, operationsGroup) != null) {
            throw new IllegalStateException("Object creation operation was preceded by some another operation");
        }
    }

    /**
     * @return stable, local timestamp of this transaction used by update
     *         operations made within this transaction
     */
    Timestamp getLocalTimestamp() {
        return localTimestamp;
    }

    /**
     * @return currently assigned global timestamp of this transaction used by
     *         update operations made within this transaction; can be null if
     *         not set
     */
    synchronized Timestamp getGlobalTimestamp() {
        return globalTimestamp;
    }

    /**
     * Assigns a (new) global timestamp to this transaction.
     * 
     * @param globalTimestamp
     *            global timestamp for this transaction
     */
    synchronized void setGlobalTimestamp(final Timestamp globalTimestamp) {
        assertStatus(TxnStatus.COMMITTED_LOCAL);
        this.globalTimestamp = globalTimestamp;
    }

    /**
     * Marks transaction as locally committed.
     */
    synchronized void markLocallyCommitted() {
        assertStatus(TxnStatus.PENDING);
        status = TxnStatus.COMMITTED_LOCAL;
    }

    /**
     * Marks transaction as globally committed, using currently assigned global
     * timestamp if it is an update transaction.
     */
    void markGloballyCommitted() {
        synchronized (this) {
            assertStatus(TxnStatus.COMMITTED_LOCAL);
            assertGlobalTimestampForUpdates(true);
            status = TxnStatus.COMMITTED_GLOBAL;
        }
        if (commitListener != null) {
            commitListener.onGlobalCommit(this);
        }
    }

    /**
     * @return true when transaction did not perform any update operation
     */
    synchronized boolean isReadOnly() {
        return localObjectOperations.isEmpty();
    }

    /**
     * @return a collection of operations group on objects updated by this
     *         transactions; these operation groups use local timestamp (
     *         {@link #getLocalTimestamp()}); the content of collection is
     *         mutable while transaction is pending; empty for read-only
     *         transaction
     */
    synchronized Collection<CRDTObjectOperationsGroup<?>> getAllLocalOperations() {
        assertStatus(TxnStatus.COMMITTED_LOCAL, TxnStatus.COMMITTED_GLOBAL);
        return localObjectOperations.values();
    }

    /**
     * @return an operations group on object updated by this transaction; this
     *         operation group uses local timestamp (
     *         {@link #getLocalTimestamp()}); null if object is not updated by
     *         this transaction
     */
    synchronized CRDTObjectOperationsGroup<?> getObjectLocalOperations(CRDTIdentifier id) {
        assertStatus(TxnStatus.COMMITTED_LOCAL, TxnStatus.COMMITTED_GLOBAL);
        return localObjectOperations.get(id);
    }

    /**
     * @return a copy of collection of operations group on objects updated by
     *         this transactions; these operation groups use a global timestamp
     *         ({@link #getGlobalTimestamp()}); empty for read-only transaction
     * @throws IllegalStateException
     *             for update transaction when global timestamp is undefined
     */
    synchronized Collection<CRDTObjectOperationsGroup<?>> getAllGlobalOperations() {
        assertStatus(TxnStatus.COMMITTED_LOCAL, TxnStatus.COMMITTED_GLOBAL);
        assertGlobalTimestampForUpdates(true);

        final List<CRDTObjectOperationsGroup<?>> objectOperationsGlobal = new LinkedList<CRDTObjectOperationsGroup<?>>();
        for (final CRDTObjectOperationsGroup<?> localGroup : localObjectOperations.values()) {
            final CRDTObjectOperationsGroup<?> globalGroup = localGroup.withBaseTimestampAndDependency(globalTimestamp,
                    updatesDependencyClock);
            objectOperationsGlobal.add(globalGroup);
        }
        return objectOperationsGlobal;
    }

    synchronized void includeGlobalDependency(final Timestamp localTs, final Timestamp globalTs) {
        assertStatus(TxnStatus.COMMITTED_LOCAL);
        assertGlobalTimestampForUpdates(false);

        for (final CRDTObjectOperationsGroup<?> ops : localObjectOperations.values()) {
            updatesDependencyClock.drop(localTs);
            updatesDependencyClock.record(globalTs);
            ops.replaceDependeeOperationTimestamp(localTs, globalTs);
        }
    }

    CausalityClock getUpdatesDependencyClock() {
        assertStatus(TxnStatus.COMMITTED_LOCAL);
        return updatesDependencyClock;
    }

    protected synchronized void assertStatus(final TxnStatus... expectedStatuses) {
        for (final TxnStatus expectedStatus : expectedStatuses) {
            if (status == expectedStatus) {
                return;
            }
        }
        throw new IllegalStateException("Unexpected transaction status: was " + status + ", expected "
                + expectedStatuses);
    }

    private void assertGlobalTimestampForUpdates(final boolean defined) {
        if (!isReadOnly() && (globalTimestamp == null == defined)) {
            throw new IllegalStateException("Global timestamp is " + (defined ? "not " : "already ") + " defined");
        }
    }
}
