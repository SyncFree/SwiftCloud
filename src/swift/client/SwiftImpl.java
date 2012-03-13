package swift.client;

import java.util.HashMap;
import java.util.Map;

import swift.client.proto.FetchObjectVersionReply;
import swift.client.proto.FetchObjectDeltaReply;
import swift.client.proto.FetchObjectDeltaRequest;
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
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.WrongTypeException;

class SwiftImpl implements Swift {
    private static final String CLIENT_CLOCK_ID = "client";
    // TODO: decide on load balancing, FT...
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
        this.latestVersion = server.getLatestKnownClock();
        this.clientTimestampGenerator = new IncrementalTimestampGenerator(CLIENT_CLOCK_ID);
    }

    // TODO offer API to prefetch/update a defined set of objects (e.g.
    // recently used), for sake of disconnected operation
    @Override
    public synchronized TxnHandle beginTxn(CachePolicy cp, boolean readOnly) {
        assertNoPendingTransaction();
        if (cp == CachePolicy.MOST_RECENT || cp == CachePolicy.STRICTLY_MOST_RECENT) {
            // FIXME: deal with communication errors
            final CausalityClock serverLatestClock = server.getLatestKnownClock();
            latestVersion.merge(serverLatestClock);
        }
        final IncrementalTripleTimestampGenerator tentativeTimestampGenerator = new IncrementalTripleTimestampGenerator(
                clientTimestampGenerator.generateNew());
        pendingTxn = new TxnHandleImpl(this, latestVersion, tentativeTimestampGenerator);
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
                refreshObject(id, version, create, classOfV, crdt);
            } else {
                throw new IllegalStateException("Client and transaction cache contain incompatible object versions");
            }
        }
        final V crdtCopy = crdt.copy(version, version);
        crdtCopy.setTxnHandle(txn);
        return crdtCopy;
    }

    private <V extends CRDT<V>> V retrieveObject(CRDTIdentifier id, CausalityClock version, boolean create,
            Class<V> classOfV) throws NoSuchObjectException, WrongTypeException {
        V crdt;
        // TODO: Support notification subscription.
        // FIXME: deal with communication errors
        final FetchObjectVersionReply state = server.fetchObjectVersion(new FetchObjectVersionRequest(id, version, false));
        if (state.isFound()) {
            if (!create) {
                throw new NoSuchObjectException("object " + id.toString() + " not found");
            }
            try {
                crdt = classOfV.newInstance();
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }
        } else {
            crdt = (V) state.getCrdt();
        }
        latestVersion.merge(state.getVersion());
        crdt.setClock(state.getVersion());
        crdt.setUID(id);
        return crdt;
    }
    
    private <V extends CRDT<V>> V refreshObject(CRDTIdentifier id, CausalityClock version, Class<V> classOfV) throws NoSuchObjectException, WrongTypeException {
        // FIXME: deal with communication errors
        final FetchObjectDeltaReply deltaReply = server.fetchObjectDelta(new FetchObjectDeltaRequest(id, crdt
                .getClock(), version, false));
        final FetchObjectVersionReply state = deltaReply.getObjectVersion();
        if (!state.isFound()) {
            crdt = (V) state.getCrdt();
        }
        latestVersion.merge(deltaReply.getVersion());
        deltaReply.crdt.merge((V) state.getCrdt());
        return crdt;
    }

    public synchronized void commitTxn(TxnHandleImpl txn) {
        assertPendingTransaction(txn);

        // Get a new timestamp.
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
