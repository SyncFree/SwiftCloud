package swift.client;

import java.util.Collection;
import java.util.HashMap;
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
    private final CausalityClock snapshotClock;
    private final Timestamp baseTimestamp;
    private final IncrementalTripleTimestampGenerator timestampSource;
    private final Map<CRDTIdentifier, TxnLocalCRDT<?>> objectsInUse;
    private final Map<CRDTIdentifier, CRDTObjectOperationsGroup> objectOperations;
    private TxnStatus status;

    TxnHandleImpl(final SwiftImpl swift, final CausalityClock snapshotClock, final Timestamp baseTimestamp) {
        this.swift = swift;
        this.snapshotClock = snapshotClock.clone();
        this.baseTimestamp = baseTimestamp;
        this.timestampSource = new IncrementalTripleTimestampGenerator(baseTimestamp);
        this.objectOperations = new HashMap<CRDTIdentifier, CRDTObjectOperationsGroup>();
        this.objectsInUse = new HashMap<CRDTIdentifier, TxnLocalCRDT<?>>();
        this.status = TxnStatus.PENDING;
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T get(CRDTIdentifier id, boolean create,
            Class<V> classOfV) throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException {
        assertPending();

        try {
            TxnLocalCRDT<V> localView = (TxnLocalCRDT<V>) objectsInUse.get(id);
            if (localView == null) {
                localView = swift.getLocalVersion(this, id, getSnapshotClock(), create, classOfV);
                objectsInUse.put(id, localView);
            }
            return (T) localView;
        } catch (ClassCastException x) {
            throw new WrongTypeException(x.getMessage());
        }
    }

    @Override
    public synchronized void commit() {
        assertPending();
        swift.commitTxn(this);
        // TODO: Support starting another transaction while the previous one is
        // currently committing at store. (COMMITTED_LOCAL). That requires some
        // serious rework on how we deal with mapping tentative timestamps to
        // server timestamps.
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

    Timestamp getBaseTimestamp() {
        return baseTimestamp;
    }

    @Override
    public synchronized void registerOperation(CRDTIdentifier id, CRDTOperation<?> op) {
        assertPending();

        CRDTObjectOperationsGroup operationsGroup = objectOperations.get(id);
        if (operationsGroup == null) {
            operationsGroup = new CRDTObjectOperationsGroup(id, getSnapshotClock(), getBaseTimestamp());
            objectOperations.put(id, operationsGroup);
        }
        operationsGroup.append(op);
    }

    synchronized void notifyLocallyCommitted() {
        assertPending();
        status = TxnStatus.COMMITTED_LOCAL;
    }

    synchronized Collection<CRDTObjectOperationsGroup> getOperations() {
        // TODO: Hmmm, perhaps COMMITTING state would be a better fit?
        return objectOperations.values();
    }

    private void assertPending() {
        if (status != TxnStatus.PENDING) {
            throw new IllegalStateException("Transaction has already terminated");
        }
    }
}
