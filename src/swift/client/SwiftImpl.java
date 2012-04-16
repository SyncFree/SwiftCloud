package swift.client;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

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
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.interfaces.TxnStatus;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcEndpoint;

/**
 * TODO: document & test
 * 
 * @author mzawirski
 */
public class SwiftImpl implements Swift, TxnManager {
    // TODO: FOR ALL REQUESTS: implement generic exponential backoff
    // retry manager+server failover?

    private static final String CLIENT_CLOCK_ID = "client";
    private static Logger logger = Logger.getLogger(SwiftImpl.class.getName());

    private static String generateClientId() {
        final Random random = new Random(System.currentTimeMillis());
        return Long.toHexString(System.identityHashCode(random) + random.nextLong());
    }

    private final String clientId;
    private final RpcEndpoint localEndpoint;
    private final Endpoint serverEndpoint;
    private final CommitterThread committerThread;
    private final ObjectsCache objectsCache;
    // Invariant: latestVersion only grows.
    private final CausalityClock latestVersion;
    // Invariant: there is at most one pending transaction.
    private TxnHandleImpl pendingTxn;
    private final LinkedList<TxnHandleImpl> locallyCommittedTxns;
    private IncrementalTimestampGenerator clientTimestampGenerator;

    public SwiftImpl(final RpcEndpoint localEndpoint, final Endpoint serverEndpoint) {
        this.clientId = generateClientId();
        this.localEndpoint = localEndpoint;
        this.serverEndpoint = serverEndpoint;
        this.objectsCache = new ObjectsCache();
        this.locallyCommittedTxns = new LinkedList<TxnHandleImpl>();
        this.latestVersion = ClockFactory.newClock();
        this.clientTimestampGenerator = new IncrementalTimestampGenerator(CLIENT_CLOCK_ID);
        this.committerThread = new CommitterThread();
        this.committerThread.start();
    }

    @Override
    public synchronized TxnHandleImpl beginTxn(CachePolicy cp, boolean readOnly) {
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
            } while (cp == CachePolicy.STRICTLY_MOST_RECENT && !doneFlag.get());
        }
        final Timestamp localTimestmap = clientTimestampGenerator.generateNew();
        // Invariant: snapshotClock (latestVersion) of a new transaction
        // dominates clock of previous transaction - monotonicity.
        setPendingTxn(new TxnHandleImpl(this, latestVersion, locallyCommittedTxns, localTimestmap));
        // FIXME: ensure that latestVersion does not contain any committing
        // transaction's global timestamp - so that the transaction will not
        // observe doubled updates (i.e. there is no locallyCommittedTxn such
        // that latestVersion.include(txn.getGlobalTimestamp())); that would
        // prevent a potential race condition caused by concurrency of
        // commitment

        logger.info("transaction " + localTimestmap + " started with snapshot point: global=" + latestVersion
                + "local=" + locallyCommittedTxns.size() + " transactions");
        return pendingTxn;
    }

    @Override
    public synchronized <V extends CRDT<V>> TxnLocalCRDT<V> getObjectTxnView(TxnHandleImpl txn, CRDTIdentifier id,
            boolean create, Class<V> classOfV) throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException {
        assertPendingTransaction(txn);

        final CausalityClock globalVisibleClock = txn.getGlobalVisibleTransactionsClock();
        V crdt = getObject(id, globalVisibleClock, create, classOfV);

        final V crdtCopy;
        final CausalityClock clockCopy;
        final Deque<TxnHandleImpl> localDependentTxns = txn.getLocalVisibleTransactions();
        if (localDependentTxns.isEmpty()) {
            crdtCopy = crdt;
            clockCopy = globalVisibleClock;
        } else {
            crdtCopy = crdt.copy();
            clockCopy = globalVisibleClock.clone();
            for (final TxnHandleImpl dependentTxn : localDependentTxns) {
                final CRDTObjectOperationsGroup<V> localOps = (CRDTObjectOperationsGroup<V>) dependentTxn
                        .getObjectLocalOperations(id);
                if (localOps != null) {
                    crdtCopy.execute(localOps, false);
                }
                clockCopy.record(dependentTxn.getLocalTimestamp());
            }
        }
        return crdtCopy.getTxnLocalCopy(clockCopy, txn);
    }

    @SuppressWarnings("unchecked")
    private <V extends CRDT<V>> V getObject(CRDTIdentifier id, CausalityClock globalVisibleClock, boolean create,
            Class<V> classOfV) throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException {
        V crdt;
        try {
            crdt = (V) objectsCache.get(id);
        } catch (ClassCastException x) {
            throw new WrongTypeException(x.getMessage());
        }

        if (crdt == null) {
            crdt = retrieveObject(id, globalVisibleClock, create, classOfV);
        } else {
            final CMP_CLOCK clockCmp = crdt.getClock().compareTo(globalVisibleClock);
            if (clockCmp == CMP_CLOCK.CMP_CONCURRENT || clockCmp == CMP_CLOCK.CMP_ISDOMINATED) {
                refreshObject(id, globalVisibleClock, classOfV, crdt);
            }
        }
        final CMP_CLOCK clockCmp = globalVisibleClock.compareTo(crdt.getPruneClock());
        if (clockCmp == CMP_CLOCK.CMP_ISDOMINATED || clockCmp == CMP_CLOCK.CMP_CONCURRENT) {
            throw new ConsistentSnapshotVersionNotFoundException(
                    "version consistent with current snapshot is not available in the store");
        }
        return crdt;
    }

    @SuppressWarnings("unchecked")
    private <V extends CRDT<V>> V retrieveObject(CRDTIdentifier id, CausalityClock globalVisibleClock, boolean create,
            Class<V> classOfV) throws NoSuchObjectException, WrongTypeException,
            ConsistentSnapshotVersionNotFoundException {
        V crdt = null;
        // TODO: Support notification subscription.

        final AtomicReference<FetchObjectVersionReply> replyRef = new AtomicReference<FetchObjectVersionReply>();
        do {
            localEndpoint.send(serverEndpoint, new FetchObjectVersionRequest(clientId, id, globalVisibleClock, false),
                    new FetchObjectVersionReplyHandler() {
                        @Override
                        public void onReceive(RpcConnection conn, FetchObjectVersionReply reply) {
                            replyRef.set(reply);
                        }
                    });
        } while (replyRef.get() == null);

        final FetchObjectVersionReply fetchReply = replyRef.get();
        final boolean presentInStore;
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
            presentInStore = false;
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
            presentInStore = true;
            break;
        default:
            throw new IllegalStateException("Unexpected status code" + fetchReply.getStatus());
        }
        crdt.init(id, fetchReply.getVersion(), fetchReply.getPruneClock(), presentInStore);
        objectsCache.add(crdt);
        latestVersion.merge(fetchReply.getVersion());
        if (fetchReply.getStatus() == FetchStatus.VERSION_NOT_FOUND) {
            throw new ConsistentSnapshotVersionNotFoundException(
                    "version consistent with current snapshot is not available in the store");
        }
        return crdt;
    }

    @SuppressWarnings("unchecked")
    private <V extends CRDT<V>> void refreshObject(CRDTIdentifier id, CausalityClock globalVisibleClock,
            Class<V> classOfV, V crdt) throws NoSuchObjectException, WrongTypeException,
            ConsistentSnapshotVersionNotFoundException {
        // WISHME: we should replace it with deltas or operations list
        final AtomicReference<FetchObjectVersionReply> replyRef = new AtomicReference<FetchObjectVersionReply>();
        do {
            localEndpoint.send(serverEndpoint, new FetchObjectDeltaRequest(clientId, id, crdt.getClock(),
                    globalVisibleClock, false), new FetchObjectVersionReplyHandler() {
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
            // TODO: this case is very special, return to it once the API is
            // stable.
            throw new ConsistentSnapshotVersionNotFoundException("consistent version is not available in the store");
        case OK:
            // Merge it with the local version.
            V receivedCrdt;
            try {
                receivedCrdt = (V) versionReply.getCrdt();
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }
            receivedCrdt.init(id, versionReply.getVersion(), versionReply.getPruneClock(), true);
            crdt.merge(receivedCrdt);
            break;
        default:
            throw new IllegalStateException("Unexpected status code" + versionReply.getStatus());
        }
        latestVersion.merge(versionReply.getVersion());
    }

    @Override
    public synchronized void discardTxn(TxnHandleImpl txn) {
        assertPendingTransaction(txn);
        setPendingTxn(null);
    }

    @Override
    public synchronized void commitTxn(TxnHandleImpl txn) {
        assertPendingTransaction(txn);

        // Big WISHME: write disk log and allow local recovery.
        txn.markLocallyCommitted();
        logger.info("transaction " + txn.getLocalTimestamp() + " commited locally");
        if (txn.isReadOnly()) {
            // Read-only transaction can be immediatelly discarded.
            txn.markGloballyCommitted();
            logger.info("read-only transaction " + txn.getLocalTimestamp() + " (virtually) commited globally");
        } else {
            // Update transaction is queued up for global commit.
            locallyCommittedTxns.addLast(txn);
        }
        setPendingTxn(null);
    }

    /**
     * Stubborn commit procedure, tries to get a global timestamp for a
     * transaction and commit using this timestamp. Repeats until it succeeds.
     * 
     * @param txn
     *            locally committed transaction
     */
    private void commitToStore(TxnHandleImpl txn) {
        txn.assertStatus(TxnStatus.COMMITTED_LOCAL);

        final AtomicReference<CommitUpdatesReply> commitReplyRef = new AtomicReference<CommitUpdatesReply>();
        do {
            requestTxnGlobalTimestamp(txn);

            final LinkedList<CRDTObjectOperationsGroup<?>> operationsGroups = new LinkedList<CRDTObjectOperationsGroup<?>>(
                    txn.getAllGlobalOperations());
            // Commit at server.
            do {
                localEndpoint.send(serverEndpoint, new CommitUpdatesRequest(clientId, txn.getGlobalTimestamp(),
                        operationsGroups), new CommitUpdatesReplyHandler() {
                    @Override
                    public void onReceive(RpcConnection conn, CommitUpdatesReply reply) {
                        commitReplyRef.set(reply);
                    }
                });
            } while (commitReplyRef.get() == null);
        } while (commitReplyRef.get().getStatus() == CommitStatus.INVALID_TIMESTAMP);

        if (commitReplyRef.get().getStatus() == CommitStatus.ALREADY_COMMITTED) {
            txn.setGlobalTimestamp(commitReplyRef.get().getCommitTimestamp());
            logger.info("transaction " + txn.getLocalTimestamp() + " is already committed using another timestamp "
                    + txn.getGlobalTimestamp());
        }
        txn.markGloballyCommitted();
        logger.info("transaction " + txn.getLocalTimestamp() + " commited globally as " + txn.getGlobalTimestamp());
    }

    /**
     * Requests and assigns a new global timestamp for the transaction.
     * 
     * @param txn
     *            locally committed transaction
     */
    private void requestTxnGlobalTimestamp(TxnHandleImpl txn) {
        txn.assertStatus(TxnStatus.COMMITTED_LOCAL);

        final AtomicReference<GenerateTimestampReply> timestampReplyRef = new AtomicReference<GenerateTimestampReply>();
        do {
            localEndpoint.send(
                    serverEndpoint,
                    new GenerateTimestampRequest(clientId, txn.getGlobalVisibleTransactionsClock(), txn
                            .getGlobalTimestamp()), new GenerateTimestampReplyHandler() {
                        @Override
                        public void onReceive(RpcConnection conn, GenerateTimestampReply reply) {
                            timestampReplyRef.set(reply);
                        }
                    });
        } while (timestampReplyRef.get() == null);

        // And replace old timestamp in operations with timestamp from server.
        txn.setGlobalTimestamp(timestampReplyRef.get().getTimestamp());
    }

    /**
     * Awaits until there is no pending transaction and applies the globally
     * committed transaction locally using a global timestamp.
     * 
     * @param txn
     *            globally committed transaction to apply locally
     */
    private synchronized void applyGlobalCommittedTxn(TxnHandleImpl txn) {
        txn.assertStatus(TxnStatus.COMMITTED_GLOBAL);

        // Globally committed transaction can only be applied when there is no
        // pending transaction.
        while (pendingTxn != null) {
            try {
                this.wait();
            } catch (InterruptedException e) {
            }
        }

        for (final CRDTObjectOperationsGroup opsGroup : txn.getAllGlobalOperations()) {
            // Try to apply changes in a cached copy of an object.
            final CRDT<?> crdt = objectsCache.get(opsGroup.getTargetUID());
            if (crdt == null) {
                logger.warning("object evicted from the local cache before global commit");
            }
            try {
                crdt.execute(opsGroup, true);
            } catch (IllegalStateException x) {
                logger.warning("cannot apply globally committed operations on local cached copy of an object - cached copy does not satisfy dependencies");
            }
        }
        if (locallyCommittedTxns.removeFirst() != txn) {
            throw new IllegalStateException("internal error, concurrently commiting transactions?");
        }
        for (final TxnHandleImpl dependingTxn : locallyCommittedTxns) {
            dependingTxn.markFirstLocalVisibleTransactionGlobal();
        }
    }

    private synchronized TxnHandleImpl getNextLocallyCommittedTxnBlocking() {
        while (locallyCommittedTxns.isEmpty()) {
            try {
                this.wait();
            } catch (InterruptedException e) {
            }
        }
        return locallyCommittedTxns.getFirst();
    }

    private synchronized void setPendingTxn(final TxnHandleImpl txn) {
        pendingTxn = txn;
        if (txn == null) {
            // Notify committer thread.
            this.notifyAll();
        }
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

    /**
     * Thread continuously committing locally committed transactions. The thread
     * takes the oldest locally committed transaction one by one and tries to
     * commit it to the store. The application of globally committed transaction
     * can only take place when there is no pending transaction.
     */
    private class CommitterThread extends Thread {

        public CommitterThread() {
            super("SwiftTransactionCommitterThread");
        }

        @Override
        public void run() {
            // TODO: introduce gentle stop()
            while (true) {
                final TxnHandleImpl nextToCommit = getNextLocallyCommittedTxnBlocking();
                commitToStore(nextToCommit);
                applyGlobalCommittedTxn(nextToCommit);
            }
        }
    }
}
