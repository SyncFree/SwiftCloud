package swift.client;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import swift.clocks.CausalityClock;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnStatus;

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
    private final CausalityClock snapshotClock;
    private final IncrementalTripleTimestampGenerator timestampSource;
    private Map<CRDTIdentifier, CRDT<?>> objectsInUse;
    private final List<CRDTOperation> operations;
    private TxnStatus status;

    TxnHandleImpl(final SwiftImpl swift, final CausalityClock snapshotClock,
            final IncrementalTripleTimestampGenerator timestampSource) {
        this.swift = swift;
        this.snapshotClock = snapshotClock;
        this.timestampSource = timestampSource;
        this.operations = new LinkedList<CRDTOperation>();
        this.objectsInUse = new HashMap<CRDTIdentifier, CRDT<?>>();
        this.status = TxnStatus.PENDING;
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfT) {
        assertPending();
        // TODO: if we want to support concurrent get()s, the impl. needs to be
        // more fancy.
        CRDT<?> crdt = objectsInUse.get(id);
        if (crdt == null) {
            crdt = swift.getObjectVersion(this, id, getSnapshotClock(), create, classOfT);
            // TODO deal with errors once they are specified
            objectsInUse.put(id, crdt);
        }
        return (V) crdt;
    }

    // TODO add getOrCreate() throws NoSuchObjectException?

    // TODO specify fail mode/timeout for get() - if we support disconnected
    // operations, it cannot be that a synchronous call fits everything.

    // TODO Additionally, more control over cache may be necessary at Swift API
    // level.

    @Override
    public synchronized void commit() {
        assertPending();
        swift.commitTxn(this);
        // TODO: Support starting another transaction while the previous one is
        // currently committing at store. (COMMITTED_LOCAL)
        status = TxnStatus.COMMITTED_STORE;
    }

    @Override
    public synchronized void rollback() {
        assertPending();
        swift.discardTxn(this);
        status = TxnStatus.CANCELLED;
    }

    public synchronized TxnStatus getStatus() {
        return status;
    }

    @Override
    public synchronized TripleTimestamp nextTimestamp() {
        assertPending();
        return timestampSource.generateNew();
    }

    @Override
    public CausalityClock getSnapshotClock() {
        return snapshotClock;
    }

    @Override
    public synchronized void registerOperation(CRDTOperation op) {
        assertPending();
        operations.add(op);
    }

    synchronized void notifyLocallyCommitted() {
        assertPending();
        status = TxnStatus.COMMITTED_LOCAL;
    }

    synchronized List<CRDTOperation> getOperations() {
        // TODO: Hmmm, perhaps COMMITTING state would be better?
        assertPending();
        return operations;
    }

    private void assertPending() {
        if (status != TxnStatus.PENDING) {
            throw new IllegalStateException("Transaction has already terminated");
        }
    }
}
