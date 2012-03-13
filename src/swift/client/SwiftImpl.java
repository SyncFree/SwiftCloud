package swift.client;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import swift.client.proto.CommitUpdatesReply;
import swift.client.proto.CommitUpdatesRequest;
import swift.client.proto.FetchObjectVersionReply;
import swift.client.proto.FetchObjectDeltaRequest;
import swift.client.proto.FetchObjectVersionRequest;
import swift.client.proto.GenerateTimestampReply;
import swift.client.proto.GenerateTimestampRequest;
import swift.client.proto.LatestKnownClockRequest;
import swift.client.proto.SwiftServer;
import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;

class SwiftImpl implements Swift {
    private static final String CLIENT_CLOCK_ID = "client";
    // TODO: declare more servers for fault tolerance.
    private final SwiftServer server;
    // TODO: implement LRU-alike eviction
    private final Map<CRDTIdentifier, CRDT<?>> objectsCache;
    private final CausalityClock latestVersion;
    // Invariant: there is at most one pending transaction.
    private TxnHandleImpl pendingTxn;
    private IncrementalTimestampGenerator clientTimestampGenerator;

    SwiftImpl(final SwiftServer server) {
        this.server = server;
        this.objectsCache = new HashMap<CRDTIdentifier, CRDT<?>>();
        this.latestVersion = ClockFactory.newClock();
        this.clientTimestampGenerator = new IncrementalTimestampGenerator(CLIENT_CLOCK_ID);
    }

    @Override
    public synchronized TxnHandle beginTxn(CachePolicy cp, boolean readOnly) {
        assertNoPendingTransaction();
        if (cp == CachePolicy.MOST_RECENT || cp == CachePolicy.STRICTLY_MOST_RECENT) {
            // FIXME: deal with communication errors
            final CausalityClock serverLatestClock = server.getLatestKnownClock(new LatestKnownClockRequest());
            latestVersion.merge(serverLatestClock);
        }
        pendingTxn = new TxnHandleImpl(this, latestVersion, clientTimestampGenerator.generateNew());
        return pendingTxn;
    }

    @SuppressWarnings("unchecked")
    public synchronized <V extends CRDT<V>> V getObjectVersion(TxnHandleImpl txn, CRDTIdentifier id,
            CausalityClock version, boolean create, Class<V> classOfV) throws WrongTypeException, NoSuchObjectException {
        assertPendingTransaction(txn);

        V crdt;
        try {
            crdt = (V) objectsCache.get(id);
        } catch (ClassCastException x) {
            throw new WrongTypeException(x.getMessage());
        }

        if (crdt == null) {
            crdt = retrieveObject(id, version, create, classOfV);
        } else {
            final CMP_CLOCK clockCmp = crdt.getClock().compareTo(version);
            if (clockCmp == CMP_CLOCK.CMP_DOMINATES || clockCmp == CMP_CLOCK.CMP_EQUALS) {
                refreshObject(id, version, classOfV, crdt);
            } else {
                // TODO LRU-eviction policy could try to avoid this happening
            }
        }
        final V crdtCopy = crdt.copy(version, version);
        crdtCopy.setTxnHandle(txn);
        return crdtCopy;
    }

    @SuppressWarnings("unchecked")
    private <V extends CRDT<V>> V retrieveObject(CRDTIdentifier id, CausalityClock version, boolean create,
            Class<V> classOfV) throws NoSuchObjectException, WrongTypeException {
        V crdt;
        // TODO: Support notification subscription.
        // FIXME: deal with communication errors
        final FetchObjectVersionReply versionReply = server.fetchObjectVersion(new FetchObjectVersionRequest(id,
                version, false));
        if (versionReply.isFound()) {
            if (!create) {
                throw new NoSuchObjectException("object " + id.toString() + " not found");
            }
            try {
                crdt = classOfV.newInstance();
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }
        } else {
            try {
                crdt = (V) versionReply.getCrdt();
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }
        }
        crdt.setClock(versionReply.getVersion());
        crdt.setUID(id);
        objectsCache.put(id, crdt);
        latestVersion.merge(versionReply.getVersion());
        return crdt;
    }

    @SuppressWarnings("unchecked")
    private <V extends CRDT<V>> void refreshObject(CRDTIdentifier id, CausalityClock version, Class<V> classOfV, V crdt)
            throws NoSuchObjectException, WrongTypeException {
        // FIXME: deal with communication errors
        // TODO: in future, we should replace it with deltas or operations list
        final FetchObjectVersionReply versionReply = server.fetchObjectDelta(new FetchObjectDeltaRequest(id, crdt
                .getClock(), version, false));
        if (!versionReply.isFound()) {
            try {
                final V newState = (V) versionReply.getCrdt();
                crdt.merge(newState);
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }
        } else {
            crdt.getClock().merge(versionReply.getVersion());
        }
        latestVersion.merge(versionReply.getVersion());
    }

    public synchronized void commitTxn(TxnHandleImpl txn) {
        assertPendingTransaction(txn);

        // Get a new timestamp.
        // FIXME: deal with communication errors
        final GenerateTimestampReply timestampReply = server.generateTimestamp(new GenerateTimestampRequest(txn
                .getSnapshotClock()));
        final Timestamp timestamp = timestampReply.getTimestamp();

        // And process the operations.
        final LinkedList<CRDTObjectOperationsGroup> operationsGroups = new LinkedList<CRDTObjectOperationsGroup>(
                txn.getOperations());
        for (final CRDTObjectOperationsGroup opsGroup : operationsGroups) {
            opsGroup.replaceBaseTimestamp(timestamp);

            // Try to apply changes in a cached copy of an object.
            final CRDT<?> crdt = objectsCache.get(opsGroup.getTargetUID());
            final CMP_CLOCK clockCompare = crdt.getClock().compareTo(opsGroup.getDependency());
            if (clockCompare == CMP_CLOCK.CMP_ISDOMINATED || clockCompare == CMP_CLOCK.CMP_CONCURRENT) {
                // TODO: ensure this won't happen?
                throw new IllegalStateException("Cached object is older/concurrent with transaction copy");
            }
            opsGroup.executeOn(crdt);
        }

        txn.notifyLocallyCommitted();
        // TODO: Support starting another transaction while the previous one is
        // currently committing at store.

        // FIXME: deal with communication errors.
        final CommitUpdatesReply commitReply = server.commitUpdates(new CommitUpdatesRequest(timestamp,
                operationsGroups));
        if (commitReply.isSuccess()) {
            // FIXME: do we assume it can ever fail? Treat in the same way as
            // communication error?
        }
        pendingTxn = null;
    }

    public synchronized void discardTxn(TxnHandleImpl txn) {
        assertPendingTransaction(txn);
        txn = null;
    }

    private void assertNoPendingTransaction() {
        if (pendingTxn != null) {
            throw new IllegalStateException("Only one transaction can be executing at the time");
        }
    }

    private void assertPendingTransaction(final TxnHandleImpl expectedTxn) {
        if (!pendingTxn.equals(expectedTxn)) {
            throw new IllegalStateException(
                    "Corrupted state: unexpected transaction is bothering me, not the pending one");
        }
    }
}
