package swift.client;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import swift.client.proto.CommitUpdatesReply;
import swift.client.proto.CommitUpdatesReply.CommitStatus;
import swift.client.proto.CommitUpdatesReplyHandler;
import swift.client.proto.CommitUpdatesRequest;
import swift.client.proto.FetchObjectVersionReply;
import swift.client.proto.FetchObjectDeltaRequest;
import swift.client.proto.FetchObjectVersionReplyHandler;
import swift.client.proto.FetchObjectVersionRequest;
import swift.client.proto.GenerateTimestampReply;
import swift.client.proto.GenerateTimestampReplyHandler;
import swift.client.proto.GenerateTimestampRequest;
import swift.client.proto.LatestKnownClockReply;
import swift.client.proto.LatestKnownClockReplyHandler;
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
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcMessage;

class SwiftImpl implements Swift {
    private static final String CLIENT_CLOCK_ID = "client";
    private final RpcEndpoint localEndpoint;
    private final Endpoint serverEndpoint;
    // TODO: implement LRU-alike eviction
    private final Map<CRDTIdentifier, CRDT<?>> objectsCache;
    private final CausalityClock latestVersion;
    // Invariant: there is at most one pending transaction.
    private TxnHandleImpl pendingTxn;
    private IncrementalTimestampGenerator clientTimestampGenerator;

    SwiftImpl(final RpcEndpoint localEndpoint, final Endpoint serverEndpoint) {
        this.localEndpoint = localEndpoint;
        this.serverEndpoint = serverEndpoint;
        this.objectsCache = new HashMap<CRDTIdentifier, CRDT<?>>();
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
        if (fetchReply.isFound()) {
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
                crdt = (V) fetchReply.getCrdt();
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }
        }
        crdt.setClock(fetchReply.getVersion());
        crdt.setUID(id);
        objectsCache.put(id, crdt);
        latestVersion.merge(fetchReply.getVersion());
        return crdt;
    }

    @SuppressWarnings("unchecked")
    private <V extends CRDT<V>> void refreshObject(CRDTIdentifier id, CausalityClock version, Class<V> classOfV, V crdt)
            throws NoSuchObjectException, WrongTypeException {
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

        // TODO: write disk log?
        txn.notifyLocallyCommitted();
        // BIG TODO: Support starting another transaction while the previous one
        // is currently committing at store. Requires some changes in core CRDT
        // classes.

        CommitStatus status;
        do {
            status = commitAtServer(txn);
        } while (status == CommitStatus.INVALID_TIMESTAMP);
        // FIXME: how about ALREADY_COMMITTED?

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

    private CommitStatus commitAtServer(TxnHandleImpl txn) {
        // Get a timestamp from server.
        final AtomicReference<GenerateTimestampReply> timestampReplyRef = new AtomicReference<GenerateTimestampReply>();
        do {
            localEndpoint.send(serverEndpoint, new GenerateTimestampRequest(txn.getSnapshotClock()),
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
        return commitReplyRef.get().getStatus();
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
