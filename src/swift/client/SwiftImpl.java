package swift.client;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import swift.client.proto.CommitUpdatesReply;
import swift.client.proto.CommitUpdatesReply.CommitStatus;
import swift.client.proto.CommitUpdatesReplyHandler;
import swift.client.proto.CommitUpdatesRequest;
import swift.client.proto.FetchObjectDeltaRequest;
import swift.client.proto.FetchObjectVersionReply;
import swift.client.proto.FetchObjectVersionReply.FetchStatus;
import swift.client.proto.FetchObjectVersionReplyHandler;
import swift.client.proto.FetchObjectVersionRequest;
import swift.client.proto.GenerateTimestampReply;
import swift.client.proto.GenerateTimestampReplyHandler;
import swift.client.proto.GenerateTimestampRequest;
import swift.client.proto.LatestKnownClockReply;
import swift.client.proto.LatestKnownClockReplyHandler;
import swift.client.proto.LatestKnownClockRequest;
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
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcEndpoint;

public class SwiftImpl implements Swift {
    private static final String CLIENT_CLOCK_ID = "client";
    private final RpcEndpoint localEndpoint;
    private final Endpoint serverEndpoint;
    // TODO: implement LRU-alike eviction
    private final ObjectsCache objectsCache;
    private final CausalityClock latestVersion;
    // Invariant: there is at most one pending transaction.
    private TxnHandleImpl pendingTxn;
    private IncrementalTimestampGenerator clientTimestampGenerator;

    public SwiftImpl(final RpcEndpoint localEndpoint, final Endpoint serverEndpoint) {
        this.localEndpoint = localEndpoint;
        this.serverEndpoint = serverEndpoint;
        this.objectsCache = new ObjectsCache();
        this.latestVersion = ClockFactory.newClock();
        this.clientTimestampGenerator = new IncrementalTimestampGenerator(CLIENT_CLOCK_ID);
    }

    @Override
    public synchronized TxnHandle beginTxn(CachePolicy cp, boolean readOnly) {
        assertNoPendingTransaction();
        if (cp == CachePolicy.MOST_RECENT || cp == CachePolicy.STRICTLY_MOST_RECENT) {
            final AtomicBoolean doneFlag = new AtomicBoolean(false);
            do {
                localEndpoint.send(serverEndpoint, new LatestKnownClockRequest(), new LatestKnownClockReplyHandler() {
                    @Override
                    public void onReceive(RpcConnection conn, LatestKnownClockReply reply) {
                        latestVersion.merge(reply.getClock());
                        doneFlag.set(true);
                    }
                });
                // TODO: FOR ALL REQUESTS: implement generic exponential backoff
                // retry manager+server failover?
            } while (cp == CachePolicy.STRICTLY_MOST_RECENT && !doneFlag.get());
        }
        pendingTxn = new TxnHandleImpl(this, latestVersion, clientTimestampGenerator.generateNew());
        return pendingTxn;
    }

    @SuppressWarnings("unchecked")
    public synchronized <V extends CRDT<V>> TxnLocalCRDT<V> getLocalVersion(TxnHandleImpl txn, CRDTIdentifier id,
            CausalityClock version, boolean create, Class<V> classOfV) throws WrongTypeException,
            NoSuchObjectException, ConsistentSnapshotVersionNotFoundException {
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
            if (clockCmp == CMP_CLOCK.CMP_CONCURRENT || clockCmp == CMP_CLOCK.CMP_ISDOMINATED) {
                refreshObject(id, version, classOfV, crdt);
            }
        }
        final CMP_CLOCK clockCmp = version.compareTo(crdt.getPruneClock());
        if (clockCmp == CMP_CLOCK.CMP_ISDOMINATED || clockCmp == CMP_CLOCK.CMP_CONCURRENT) {
            throw new ConsistentSnapshotVersionNotFoundException(
                    "version consistent with current snapshot is not available in the store");
        }
        final TxnLocalCRDT<V> crdtCopy = crdt.getTxnLocalCopy(version, txn);
        return crdtCopy;
    }

    @SuppressWarnings("unchecked")
    private <V extends CRDT<V>> V retrieveObject(CRDTIdentifier id, CausalityClock version, boolean create,
            Class<V> classOfV) throws NoSuchObjectException, WrongTypeException,
            ConsistentSnapshotVersionNotFoundException {
        V crdt = null;
        // TODO: Support notification subscription.

        final AtomicReference<FetchObjectVersionReply> replyRef = new AtomicReference<FetchObjectVersionReply>();
        do {
            localEndpoint.send(serverEndpoint, new FetchObjectVersionRequest(id, version, false),
                    new FetchObjectVersionReplyHandler() {
                        @Override
                        public void onReceive(RpcConnection conn, FetchObjectVersionReply reply) {
                            replyRef.set(reply);
                        }
                    });
        } while (replyRef.get() == null);

        final FetchObjectVersionReply fetchReply = replyRef.get();
        switch (fetchReply.getStatus()) {
        case OBJECT_NOT_FOUND:
            if (!create) {
                throw new NoSuchObjectException("object " + id.toString() + " not found");
            }
            try {
                crdt = classOfV.newInstance();
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }
            break;
        case VERSION_NOT_FOUND:
            // Even though we cannot satisfy application's request, save the
            // object for sake of restarted transaction.
        case OK:
            try {
                crdt = (V) fetchReply.getCrdt();
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }
            break;
        default:
            throw new IllegalStateException("Unexpected status code" + fetchReply.getStatus());
        }
        crdt.setClock(fetchReply.getVersion());
        crdt.setPruneClock(fetchReply.getPruneClock());
        crdt.setUID(id);
        objectsCache.add(crdt);
        latestVersion.merge(fetchReply.getVersion());
        if (fetchReply.getStatus() == FetchStatus.VERSION_NOT_FOUND) {
            throw new ConsistentSnapshotVersionNotFoundException(
                    "version consistent with current snapshot is not available in the store");
        }
        return crdt;
    }

    @SuppressWarnings("unchecked")
    private <V extends CRDT<V>> void refreshObject(CRDTIdentifier id, CausalityClock version, Class<V> classOfV, V crdt)
            throws NoSuchObjectException, WrongTypeException, ConsistentSnapshotVersionNotFoundException {
        // WISHME: we should replace it with deltas or operations list
        final AtomicReference<FetchObjectVersionReply> replyRef = new AtomicReference<FetchObjectVersionReply>();
        do {
            localEndpoint.send(serverEndpoint, new FetchObjectDeltaRequest(id, crdt.getClock(), version, false),
                    new FetchObjectVersionReplyHandler() {
                        @Override
                        public void onReceive(RpcConnection conn, FetchObjectVersionReply reply) {
                            replyRef.set(reply);
                        }
                    });
        } while (replyRef.get() == null);

        final FetchObjectVersionReply versionReply = replyRef.get();
        switch (versionReply.getStatus()) {
        case OBJECT_NOT_FOUND:
            // Just update the clock of local version.
            crdt.getClock().merge(versionReply.getVersion());
            break;
        case VERSION_NOT_FOUND:
            // Do not merge, since we would lost some versioning information
            // that client relies on.
            throw new ConsistentSnapshotVersionNotFoundException("consistent version is not available in the store");
        case OK:
            // Merge it with the local version.
            V receivedCrdt;
            try {
                receivedCrdt = (V) versionReply.getCrdt();
                //MAREK: check if this is correct
                receivedCrdt.setClock(versionReply.getVersion());
                receivedCrdt.setPruneClock(versionReply.getPruneClock());
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }
            crdt.merge(receivedCrdt);
            break;
        default:
            throw new IllegalStateException("Unexpected status code" + versionReply.getStatus());
        }
        latestVersion.merge(versionReply.getVersion());
    }

    public synchronized void commitTxn(TxnHandleImpl txn) {
        assertPendingTransaction(txn);

        // TODO: write disk log?
        txn.notifyLocallyCommitted();
        // BIG TODO: Support starting another transaction while the previous one
        // is currently committing at store. Requires some changes in core CRDT
        // classes.

        CommitUpdatesReply commitReply = null;
        do {
            commitReply = commitAtServer(txn, commitReply == null ? null : commitReply.getCommitTimestamp());
        } while (commitReply.getStatus() == CommitStatus.INVALID_TIMESTAMP);

        if (commitReply.getStatus() == CommitStatus.ALREADY_COMMITTED) {
            // FIXME: replace timestamps to getPreviousTimestamp()
        }

        for (final CRDTObjectOperationsGroup opsGroup : txn.getOperations()) {
            // Try to apply changes in a cached copy of an object.
            final CRDT<?> crdt = objectsCache.get(opsGroup.getTargetUID());
            final CMP_CLOCK clockCompare = crdt.getClock().compareTo(opsGroup.getDependency());
            if (clockCompare == CMP_CLOCK.CMP_ISDOMINATED || clockCompare == CMP_CLOCK.CMP_CONCURRENT) {
                // TODO: LRU eviction: ensure this won't happen?
                throw new IllegalStateException("Cached object is older/concurrent with transaction copy");
            }
            opsGroup.executeOn(crdt);
        }
        pendingTxn = null;
    }

    private CommitUpdatesReply commitAtServer(TxnHandleImpl txn, final Timestamp previousTimestamp) {
        // Get a timestamp from server.
        final AtomicReference<GenerateTimestampReply> timestampReplyRef = new AtomicReference<GenerateTimestampReply>();
        do {
            localEndpoint.send(serverEndpoint, new GenerateTimestampRequest(txn.getSnapshotClock(), previousTimestamp),
                    new GenerateTimestampReplyHandler() {
                        @Override
                        public void onReceive(RpcConnection conn, GenerateTimestampReply reply) {
                            timestampReplyRef.set(reply);
                        }
                    });
        } while (timestampReplyRef.get() == null);

        // And replace old timestamp in operations with timestamp from server.
        final Timestamp timestamp = timestampReplyRef.get().getTimestamp();
        final LinkedList<CRDTObjectOperationsGroup> operationsGroups = new LinkedList<CRDTObjectOperationsGroup>(
                txn.getOperations());
        for (final CRDTObjectOperationsGroup opsGroup : operationsGroups) {
            opsGroup.replaceBaseTimestamp(timestamp);
        }

        // Commit at server.
        final AtomicReference<CommitUpdatesReply> commitReplyRef = new AtomicReference<CommitUpdatesReply>();
        do {
            localEndpoint.send(serverEndpoint, new CommitUpdatesRequest(timestamp, operationsGroups),
                    new CommitUpdatesReplyHandler() {
                        @Override
                        public void onReceive(RpcConnection conn, CommitUpdatesReply reply) {
                            commitReplyRef.set(reply);
                        }
                    });
        } while (commitReplyRef.get() == null);

        // FIXME: look for getCommitTImestamp()
        return commitReplyRef.get();
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
