package swift.client;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import swift.clocks.CausalityClock;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.interfaces.TxnStatus;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

/**
 * Implementation of SwiftCloud transaction, keeps track of its state and
 * mediates between application and low-level SwiftImpl client.
 * <p>
 * Instances shall be generated using and managed by Swift client implementation
 * only. Thread-safe.
 * 
 * @author mzawirski
 */
class TxnHandleImpl implements TxnHandle {
    private final SwiftImpl swift;
    private final CausalityClock globalVisibleTransactionsClock;
    private final LinkedList<TxnHandleImpl> localVisibleTransactions;
    private final Timestamp localTimestamp;
    private final IncrementalTripleTimestampGenerator timestampSource;
    private Timestamp globalTimestamp;
    private final Map<CRDTIdentifier, TxnLocalCRDT<?>> objectsInUse;
    private final Map<CRDTIdentifier, CRDTObjectOperationsGroup<?>> localObjectOperations;
    private TxnStatus status;

    TxnHandleImpl(final SwiftImpl swift, final CausalityClock globalVisibleTransactionsClock,
            final List<TxnHandleImpl> localVisibleTransactions, final Timestamp localTimestamp) {
        this.swift = swift;
        this.globalVisibleTransactionsClock = globalVisibleTransactionsClock.clone();
        this.localVisibleTransactions = new LinkedList<TxnHandleImpl>(localVisibleTransactions);
        this.localTimestamp = localTimestamp;
        this.timestampSource = new IncrementalTripleTimestampGenerator(localTimestamp);
        this.localObjectOperations = new HashMap<CRDTIdentifier, CRDTObjectOperationsGroup<?>>();
        this.objectsInUse = new HashMap<CRDTIdentifier, TxnLocalCRDT<?>>();
        this.status = TxnStatus.PENDING;
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T get(CRDTIdentifier id, boolean create,
            Class<V> classOfV) throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException {
        assertStatus(TxnStatus.PENDING);

        try {
            TxnLocalCRDT<V> localView = (TxnLocalCRDT<V>) objectsInUse.get(id);
            if (localView == null) {
                localView = swift.getObjectTxnView(this, id, create, classOfV);
                objectsInUse.put(id, localView);
            }
            return (T) localView;
        } catch (ClassCastException x) {
            throw new WrongTypeException(x.getMessage());
        }
    }

    @Override
    public synchronized void commit(boolean waitForGlobalCommit) {
        assertStatus(TxnStatus.PENDING);
        swift.commitTxn(this, waitForGlobalCommit);
    }

    @Override
    public synchronized void rollback() {
        assertStatus(TxnStatus.PENDING);
        swift.discardTxn(this);
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

    synchronized List<TxnHandleImpl> getLocalVisibleTransactions() {
        return localVisibleTransactions;
    }

    synchronized CausalityClock getGlobalVisibleTransactionsClock() {
        return globalVisibleTransactionsClock;
    }

    synchronized CausalityClock getAllVisibleTransactionsClock() {
        final CausalityClock clock = globalVisibleTransactionsClock.clone();
        for (final TxnHandleImpl txn : localVisibleTransactions) {
            clock.record(txn.getLocalTimestamp());
        }
        return clock;
    }

    Timestamp getLocalTimestamp() {
        return localTimestamp;
    }

    synchronized Timestamp getGlobalTimestamp() {
        return globalTimestamp;
    }

    synchronized void markFirstLocalVisibleTransactionGlobal() {
        assertStatus(TxnStatus.COMMITTED_LOCAL);

        final TxnHandleImpl txn = localVisibleTransactions.removeFirst();
        txn.assertStatus(TxnStatus.COMMITTED_GLOBAL);
        final Timestamp oldTs = txn.getLocalTimestamp();
        final Timestamp newTs = txn.getGlobalTimestamp();
        globalVisibleTransactionsClock.record(newTs);

        for (final CRDTObjectOperationsGroup<?> ops : localObjectOperations.values()) {
            ops.replaceDependentTimestamp(oldTs, newTs);
        }
    }

    synchronized void setGlobalTimestamp(final Timestamp globalTimestamp) {
        assertStatus(TxnStatus.COMMITTED_LOCAL);
        if (!localVisibleTransactions.isEmpty()) {
            throw new IllegalStateException("There is a local dependent transaction that was not globally committed");
        }
        this.globalTimestamp = globalTimestamp;
    }

    @Override
    public synchronized <V extends CRDT<V>> void registerOperation(CRDTIdentifier id, CRDTOperation<V> op) {
        assertStatus(TxnStatus.PENDING);

        @SuppressWarnings("unchecked")
        CRDTObjectOperationsGroup<V> operationsGroup = (CRDTObjectOperationsGroup<V>) localObjectOperations.get(id);
        if (operationsGroup == null) {
            operationsGroup = new CRDTObjectOperationsGroup<V>(id, getAllVisibleTransactionsClock(),
                    getLocalTimestamp(), null);
            localObjectOperations.put(id, operationsGroup);
        }
        operationsGroup.append(op);
    }

    @Override
    public synchronized <V extends CRDT<V>> void registerObjectCreation(CRDTIdentifier id, V creationState) {
        final CRDTObjectOperationsGroup<V> operationsGroup = new CRDTObjectOperationsGroup<V>(id,
                getAllVisibleTransactionsClock(), getLocalTimestamp(), creationState);
        if (localObjectOperations.put(id, operationsGroup) != null) {
            throw new IllegalStateException("Object creation operation was preceded by some another operation");
        }
    }

    synchronized void markLocallyCommitted() {
        assertStatus(TxnStatus.PENDING);
        status = TxnStatus.COMMITTED_LOCAL;
    }

    synchronized void markGloballyCommitted() {
        assertStatus(TxnStatus.COMMITTED_LOCAL);
        status = TxnStatus.COMMITTED_GLOBAL;
        // TODO: add async. notification to the application
    }

    synchronized Collection<CRDTObjectOperationsGroup<?>> getAllLocalOperations() {
        return localObjectOperations.values();
    }

    synchronized CRDTObjectOperationsGroup<?> getObjectLocalOperations(CRDTIdentifier id) {
        return localObjectOperations.get(id);
    }

    synchronized Collection<CRDTObjectOperationsGroup<?>> getAllGlobalOperations() {
        final List<CRDTObjectOperationsGroup<?>> objectOperationsGlobal = new LinkedList<CRDTObjectOperationsGroup<?>>();
        for (final CRDTObjectOperationsGroup<?> localGroup : localObjectOperations.values()) {
            objectOperationsGlobal.add(localGroup.withBaseTimestamp(globalTimestamp));
        }
        return objectOperationsGlobal;
    }

    private void assertStatus(final TxnStatus expectedStatus) {
        if (status != expectedStatus) {
            throw new IllegalStateException("Unexpected transaction status: was " + status + ", expected "
                    + expectedStatus);
        }
    }
}
