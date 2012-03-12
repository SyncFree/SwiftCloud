package swift.client;

import java.util.HashMap;
import java.util.Map;

import swift.client.proto.CRDTState;
import swift.client.proto.FetchObjectVersionRequest;
import swift.client.proto.GenerateTimestampRequest;
import swift.client.proto.SwiftServer;
import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;

class SwiftImpl implements Swift {
    private static final String CLIENT_CLOCK_ID = "client";
    // TODO: decide on load balancing, FT...
    private final SwiftServer server;
    private final Map<CRDTIdentifier, CRDT<?>> objectsCache;
    private final CausalityClock latestVersion;
    // Invariant: there is at most one pending transaction.
    private TxnHandleImpl pendingTxn;
    private IncrementalTimestampGenerator clientTimestampGenerator;

    SwiftImpl(final SwiftServer server) {
        this.server = server;
        this.objectsCache = new HashMap<CRDTIdentifier, CRDT<?>>();
        this.latestVersion = server.getLatestClock();
        this.clientTimestampGenerator = new IncrementalTimestampGenerator(CLIENT_CLOCK_ID);
    }

    @Override
    public synchronized TxnHandle beginTxn(CachePolicy cp, boolean readOnly) {
        assertNoPendingTransaction();
        if (cp == CachePolicy.MOST_RECENT || cp == CachePolicy.STRICTLY_MOST_RECENT) {
            // TODO handle failures...
            final CausalityClock serverLatestClock = server.getLatestClock();
            // TODO offer API to prefetch/update a defined set of objects (e.g.
            // recently used),
            // to allow disconnected operation
            latestVersion.merge(serverLatestClock);
        }
        final IncrementalTripleTimestampGenerator tentativeTimestampGenerator = new IncrementalTripleTimestampGenerator(
                clientTimestampGenerator.generateNew());
        pendingTxn = new TxnHandleImpl(this, latestVersion, tentativeTimestampGenerator);
        return pendingTxn;
    }

    public synchronized CRDT<?> getObjectVersion(TxnHandleImpl txn, CRDTIdentifier id, CausalityClock version,
            boolean create) {
        assertPendingTransaction(txn);

        CRDT<?> crdt = objectsCache.get(id);
        if (crdt == null) {
            // TODO: Support notification subscription.
            final CRDTState state = server
                    .fetchObjectVersion(new FetchObjectVersionRequest(id, version, create, false));
            crdt = state.getCrdt();
            crdt.setClock(state.getVersion());
        }
        // FIXME: make a copy (possibly pruned and restricted to a version)
        final CRDT<?> crdtCopy = crdt;
        crdtCopy.setTxnHandle(txn);
        return crdtCopy;
    }

    public synchronized void commitTxn(TxnHandleImpl txn) {
        assertPendingTransaction(txn);

        // Get a new timestamp and replace it.
        final Timestamp timestamp = server.generateTimestamp(new GenerateTimestampRequest(txn.getSnapshotClock()));
        // Replace tentative timestamps.
        for (final CRDTOperation op : txn.getOperations()) {
            op.replaceBaseTimestamp(timestamp);

            // Try to apply changes in a cached copy of an object.
            final CRDT<?> crdt = objectsCache.get(op.getTargetUID());
            // TODO: deal with the case when object is not in the cache
            final CMP_CLOCK clockCompare = crdt.getClock().compareTo(op.getDependency());
            if (clockCompare == CMP_CLOCK.CMP_ISDOMINATED || clockCompare == CMP_CLOCK.CMP_CONCURRENT) {
                throw new IllegalStateException("Cached object is older/concurrent with transaction copy");
            }
            crdt.executeOperation(op);
        }

        txn.notifyLocallyCommitted();
        // TODO: Support starting another transaction while the previous one is
        // currently committing at store.

        // FIXME: send updates to the server :-)
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
