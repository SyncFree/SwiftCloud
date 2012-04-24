package swift.client;

import static sys.net.api.Networking.Networking;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import swift.client.proto.CommitUpdatesReply;
import swift.client.proto.CommitUpdatesReply.CommitStatus;
import swift.client.proto.CommitUpdatesReplyHandler;
import swift.client.proto.CommitUpdatesRequest;
import swift.client.proto.FastRecentUpdatesReply;
import swift.client.proto.FastRecentUpdatesReplyHandler;
import swift.client.proto.FastRecentUpdatesRequest;
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
import swift.client.proto.SubscriptionType;
import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperationDependencyPolicy;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.ObjectUpdatesListener;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.interfaces.TxnStatus;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import swift.exceptions.ConsistentSnapshotVersionNotFoundException;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.WrongTypeException;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcEndpoint;

/**
 * Implementation of Swift client and transactions manager.
 * 
 * @see Swift, TxnManager
 * 
 * @author mzawirski
 */
public class SwiftImpl implements Swift, TxnManager {
    // TODO: cache eviction and pruning
    // TODO: server failover
    public static int DEFAULT_TIMEOUT_MILLIS = 10000;
    private static final String CLIENT_CLOCK_ID = "client";
    private static Logger logger = Logger.getLogger(SwiftImpl.class.getName());

    /**
     * Creates new instance of Swift using provided network settings and default
     * cache parameters.
     * 
     * @param localPort
     *            port to bind local RPC endpoint
     * @param serverHostname
     *            hostname of storage server
     * @param serverPort
     *            TCP port of storage server
     * @return instance of Swift client
     */
    public static SwiftImpl newInstance(int localPort, String serverHostname, int serverPort) {
        return new SwiftImpl(Networking.rpcBind(localPort, null), Networking.resolve(serverHostname, serverPort),
                new InfiniteObjectsCache(), DEFAULT_TIMEOUT_MILLIS);
    }

    private static String generateClientId() {
        final Random random = new Random(System.currentTimeMillis());
        return Long.toHexString(System.identityHashCode(random) + random.nextLong());
    }

    private boolean stopFlag;
    private boolean stopGracefully;
    private final String clientId;
    private final RpcEndpoint localEndpoint;
    private final Endpoint serverEndpoint;
    private final CommitterThread committerThread;
    // Cache of objects.
    // Invariant: if object is in the cache, it must include all updates
    // of locally and globally committed locally-originating transactions.
    private final InfiniteObjectsCache objectsCache;
    // Invariant: latestVersion only grows.
    private final CausalityClock latestVersion;
    // Invariant: there is at most one pending (open) transaction.
    private AbstractTxnHandle pendingTxn;
    // Locally committed transactions (in commit order), the first one is
    // possibly committing to the store.
    private final LinkedList<AbstractTxnHandle> locallyCommittedTxnsQueue;
    // Local dependencies of a pending transaction.
    private final LinkedList<AbstractTxnHandle> pendingTxnLocalDependencies;
    private final Map<CRDTIdentifier, ObjectUpdatesSubscription> subscribedObjectsAccesses;
    private final NotoficationsProcessorThread notificationsThread;
    private IncrementalTimestampGenerator clientTimestampGenerator;
    private final int timeoutMillis;

    SwiftImpl(final RpcEndpoint localEndpoint, final Endpoint serverEndpoint, InfiniteObjectsCache objectsCache,
            int timeoutMillis) {
        this.clientId = generateClientId();
        this.timeoutMillis = timeoutMillis;
        this.localEndpoint = localEndpoint;
        this.serverEndpoint = serverEndpoint;
        this.objectsCache = objectsCache;
        this.locallyCommittedTxnsQueue = new LinkedList<AbstractTxnHandle>();
        this.pendingTxnLocalDependencies = new LinkedList<AbstractTxnHandle>();
        this.latestVersion = ClockFactory.newClock();
        this.clientTimestampGenerator = new IncrementalTimestampGenerator(CLIENT_CLOCK_ID);
        this.committerThread = new CommitterThread();
        this.committerThread.start();
        this.subscribedObjectsAccesses = new HashMap<CRDTIdentifier, ObjectUpdatesSubscription>();
        this.notificationsThread = new NotoficationsProcessorThread();
        this.notificationsThread.start();
    }

    @Override
    public void stop(boolean waitForCommit) {
        synchronized (this) {
            stopFlag = true;
            stopGracefully = waitForCommit;
            this.notifyAll();
        }
        try {
            committerThread.join();
        } catch (InterruptedException e) {
            logger.warning(e.getMessage());
        }
    }

    @Override
    public synchronized AbstractTxnHandle beginTxn(IsolationLevel isolationLevel, CachePolicy cachePolicy,
            boolean readOnly) throws NetworkException {
        // FIXME: Ooops, readOnly is present here at API level, respect it here
        // and in TxnHandleImpl or remove it from API.
        assertNoPendingTransaction();
        assertRunning();

        switch (isolationLevel) {
        case SNAPSHOT_ISOLATION:
            if (cachePolicy == CachePolicy.MOST_RECENT || cachePolicy == CachePolicy.STRICTLY_MOST_RECENT) {
                final AtomicBoolean doneFlag = new AtomicBoolean(false);
                localEndpoint.send(serverEndpoint, new LatestKnownClockRequest(clientId),
                        new LatestKnownClockReplyHandler() {
                            @Override
                            public void onReceive(RpcConnection conn, LatestKnownClockReply reply) {
                                updateLatestKnownClock(reply.getClock());
                                doneFlag.set(true);
                            }
                        }, timeoutMillis);
                if (!doneFlag.get() && cachePolicy == CachePolicy.STRICTLY_MOST_RECENT) {
                    throw new NetworkException("timed out to get transcation snapshot point");
                }
            }
            final Timestamp localTimestmap = clientTimestampGenerator.generateNew();
            // Invariant: for SI snapshotClock of a new transaction dominates
            // clock of all previous SI transaction (monotonic reads), since
            // latestVersion only grows.
            final CausalityClock snapshotClock = latestVersion.clone();
            setPendingTxn(new SnapshotIsolationTxnHandle(this, cachePolicy, localTimestmap, snapshotClock));
            logger.info("SI transaction " + localTimestmap + " started with global snapshot point: " + snapshotClock);
            return pendingTxn;
        default:
            // FIXME: implement other isolation levels.
            throw new UnsupportedOperationException("isolation level " + isolationLevel + " unsupported");
        }
    }

    private synchronized void updateLatestKnownClock(final CausalityClock clock) {
        if (clock == null) {
            logger.warning("server returned null clock");
            return;
        }

        latestVersion.merge(clock);
        final AbstractTxnHandle commitingTxn = locallyCommittedTxnsQueue.peekFirst();
        if (commitingTxn != null && clock.includes(commitingTxn.getGlobalTimestamp())) {
            // We have received global update of locally committed transaction
            // before it was locally recognized as globally committed.
            applyGloballyCommittedTxn(commitingTxn);
        }
    }

    @Override
    public synchronized <V extends CRDT<V>> TxnLocalCRDT<V> getObjectTxnView(AbstractTxnHandle txn, CRDTIdentifier id,
            CausalityClock minVersion, final boolean tryMoreRecent, boolean create, Class<V> classOfV,
            final ObjectUpdatesListener updatesListener) throws WrongTypeException, NoSuchObjectException,
            ConsistentSnapshotVersionNotFoundException, NetworkException {
        assertPendingTransaction(txn);
        if (minVersion.getLatestCounter(CLIENT_CLOCK_ID) != Timestamp.MIN_VALUE) {
            throw new IllegalArgumentException("transaction requested visibility of local transaction");
        }

        // FIXME honor tryMoreRecent
        TxnLocalCRDT<V> localView = getCachedObjectForTxn(id, minVersion, classOfV);
        if (localView != null) {
            return localView;
        }

        // FIXME: support updatesListener for real
        final CausalityClock clock = clockWithLocalDependencies(minVersion);
        fetchLatestObject(id, create, classOfV, clock, updatesListener != null);

        localView = getCachedObjectForTxn(id, minVersion, classOfV);
        if (localView == null) {
            throw new IllegalStateException(
                    "Internal error: recently retrieved object unavailable in appropriate version in the cache");
        }
        return localView;
    }

    private <V extends CRDT<V>> void fetchLatestObject(CRDTIdentifier id, boolean create, Class<V> classOfV,
            CausalityClock minVersion, final boolean subscribeUpdates) throws WrongTypeException,
            NoSuchObjectException, ConsistentSnapshotVersionNotFoundException, NetworkException {
        final V crdt;
        try {
            crdt = (V) objectsCache.get(id);
        } catch (ClassCastException x) {
            throw new WrongTypeException(x.getMessage());
        }

        if (crdt == null) {
            fetchObject(id, create, classOfV, minVersion, subscribeUpdates);
        } else {
            refreshObject(id, classOfV, crdt, minVersion, subscribeUpdates);
        }
    }

    private CausalityClock clockWithLocalDependencies(CausalityClock clock) {
        clock = clock.clone();
        for (final AbstractTxnHandle dependentTxn : pendingTxnLocalDependencies) {
            // Include in clock those dependent transactions that already
            // committed globally after the pending transaction started.
            if (dependentTxn.getStatus() == TxnStatus.COMMITTED_GLOBAL) {
                clock.record(dependentTxn.getGlobalTimestamp());
            } else {
                clock.record(dependentTxn.getLocalTimestamp());
            }
        }
        return clock;
    }

    @SuppressWarnings("unchecked")
    private <V extends CRDT<V>> TxnLocalCRDT<V> getCachedObjectForTxn(CRDTIdentifier id, CausalityClock clock,
            Class<V> classOfV) throws WrongTypeException, ConsistentSnapshotVersionNotFoundException {
        V crdt;
        try {
            crdt = (V) objectsCache.get(id);
        } catch (ClassCastException x) {
            throw new WrongTypeException(x.getMessage());
        }
        if (crdt == null) {
            return null;
        }

        if (clock == null) {
            // Return the most recent version.
            clock = crdt.getClock();
        }
        clock = clockWithLocalDependencies(clock);
        final CausalityClock globalClock = clock.clone();
        globalClock.drop(CLIENT_CLOCK_ID);

        final CMP_CLOCK clockCmp = crdt.getClock().compareTo(globalClock);
        if (clockCmp == CMP_CLOCK.CMP_CONCURRENT || clockCmp == CMP_CLOCK.CMP_ISDOMINATED) {
            return null;
        }

        final CMP_CLOCK pruneCmp = globalClock.compareTo(crdt.getPruneClock());
        if (pruneCmp == CMP_CLOCK.CMP_ISDOMINATED || pruneCmp == CMP_CLOCK.CMP_CONCURRENT) {
            throw new ConsistentSnapshotVersionNotFoundException(
                    "version consistent with current snapshot is not available in the store");
            // TODO: Or just in the cache? return to it if server returns pruned
            // crdt.
        }

        final TxnLocalCRDT<V> crdtView;
        // Are there any local dependencies to apply on the cached object?
        if (clock.getLatestCounter(CLIENT_CLOCK_ID) != Timestamp.MIN_VALUE) {
            // Apply them on sandboxed copy of an object, since these operations
            // use local timestamps.
            final V crdtCopy = crdt.copy();
            for (final AbstractTxnHandle dependentTxn : pendingTxnLocalDependencies) {
                final CRDTObjectOperationsGroup<V> localOps;
                try {
                    localOps = (CRDTObjectOperationsGroup<V>) dependentTxn.getObjectLocalOperations(id);
                } catch (ClassCastException x) {
                    throw new WrongTypeException(x.getMessage());
                }
                if (localOps != null) {
                    crdtCopy.execute(localOps, CRDTOperationDependencyPolicy.IGNORE);
                }
            }
            crdtView = crdtCopy.getTxnLocalCopy(clock, pendingTxn);
        } else {
            crdtView = crdt.getTxnLocalCopy(clock, pendingTxn);
        }
        return crdtView;
    }

    @SuppressWarnings("unchecked")
    private <V extends CRDT<V>> V fetchObject(CRDTIdentifier id, boolean create, Class<V> classOfV,
            CausalityClock minVersion, boolean subscribeUpdates) throws NoSuchObjectException, WrongTypeException,
            ConsistentSnapshotVersionNotFoundException, NetworkException {
        V crdt = null;

        final AtomicReference<FetchObjectVersionReply> replyRef = new AtomicReference<FetchObjectVersionReply>();
        final SubscriptionType subscriptionType = subscribeUpdates ? SubscriptionType.UPDATES : SubscriptionType.NONE;
        localEndpoint.send(serverEndpoint, new FetchObjectVersionRequest(clientId, id, minVersion, true, true,
                subscriptionType), new FetchObjectVersionReplyHandler() {
            @Override
            public void onReceive(RpcConnection conn, FetchObjectVersionReply reply) {
                replyRef.set(reply);
            }
        }, timeoutMillis);
        if (replyRef.get() == null) {
            throw new NetworkException("Fetching object version timed out");
        }

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
        final CausalityClock pruneClock;
        if (fetchReply.getPruneClock() != null) {
            pruneClock = fetchReply.getPruneClock();
        } else {
            pruneClock = ClockFactory.newClock();
        }
        crdt.init(id, fetchReply.getVersion(), pruneClock, presentInStore);
        objectsCache.add(crdt);
        updateLatestKnownClock(fetchReply.getVersion());
        if (fetchReply.getStatus() == FetchStatus.VERSION_NOT_FOUND) {
            logger.warning("unexpected server reply - version not found while version was left unspecified");
        }
        return crdt;
    }

    @SuppressWarnings("unchecked")
    private <V extends CRDT<V>> void refreshObject(CRDTIdentifier id, Class<V> classOfV, V crdt,
            CausalityClock minVersion, boolean subscribeUpdates) throws NoSuchObjectException, WrongTypeException,
            ConsistentSnapshotVersionNotFoundException, NetworkException {
        // WISHME: we should replace it with deltas or operations list
        final AtomicReference<FetchObjectVersionReply> replyRef = new AtomicReference<FetchObjectVersionReply>();
        final SubscriptionType subscriptionType = subscribeUpdates ? SubscriptionType.UPDATES : SubscriptionType.NONE;
        localEndpoint.send(serverEndpoint, new FetchObjectDeltaRequest(clientId, id, crdt.getClock(), minVersion, true,
                true, subscriptionType), new FetchObjectVersionReplyHandler() {
            @Override
            public void onReceive(RpcConnection conn, FetchObjectVersionReply reply) {
                replyRef.set(reply);
            }
        }, timeoutMillis);
        if (replyRef.get() == null) {
            throw new NetworkException("Fetching newer object version timed out");
        }

        final FetchObjectVersionReply versionReply = replyRef.get();
        switch (versionReply.getStatus()) {
        case OBJECT_NOT_FOUND:
            // Just update the clock of local version.
            crdt.getClock().merge(versionReply.getVersion());
            break;
        case VERSION_NOT_FOUND:
            logger.warning("unexpected server reply - version not found while requested more recent updates ");
            break;
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
        updateLatestKnownClock(versionReply.getVersion());
    }

    @Override
    public synchronized void discardTxn(AbstractTxnHandle txn) {
        assertPendingTransaction(txn);
        setPendingTxn(null);
    }

    @Override
    public synchronized void commitTxn(AbstractTxnHandle txn) {
        assertPendingTransaction(txn);
        assertRunning();

        // Big WISHME: write disk log and allow local recovery.
        txn.markLocallyCommitted();
        logger.info("transaction " + txn.getLocalTimestamp() + " commited locally");
        if (txn.isReadOnly()) {
            // Read-only transaction can be immediately discarded.
            txn.markGloballyCommitted();
            logger.info("read-only transaction " + txn.getLocalTimestamp() + " (virtually) commited globally");
        } else {
            for (final AbstractTxnHandle dependeeTxn : pendingTxnLocalDependencies) {
                // Replace timestamps of transactions that globally committed
                // when this transaction was pending.
                if (dependeeTxn.getStatus() == TxnStatus.COMMITTED_GLOBAL) {
                    txn.includeGlobalDependency(dependeeTxn.getLocalTimestamp(), dependeeTxn.getGlobalTimestamp());
                }
            }
            // Update transaction is queued up for global commit.
            addLocallyCommittedTransaction(txn);
        }
        setPendingTxn(null);
    }

    private void addLocallyCommittedTransaction(AbstractTxnHandle txn) {
        locallyCommittedTxnsQueue.addLast(txn);
        // Notify committer thread.
        this.notifyAll();
    }

    /**
     * Stubborn commit procedure, tries to get a global timestamp for a
     * transaction and commit using this timestamp. Repeats until it succeeds.
     * 
     * @param txn
     *            locally committed transaction
     */
    private void commitToStore(AbstractTxnHandle txn) {
        txn.assertStatus(TxnStatus.COMMITTED_LOCAL);
        if (txn.getUpdatesDependencyClock().getLatestCounter(CLIENT_CLOCK_ID) != Timestamp.MIN_VALUE) {
            throw new IllegalStateException("Trying to commit to data store with client clock");
        }

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
                }, timeoutMillis);
            } while (commitReplyRef.get() == null);
        } while (commitReplyRef.get().getStatus() == CommitStatus.INVALID_TIMESTAMP);

        if (commitReplyRef.get().getStatus() == CommitStatus.ALREADY_COMMITTED) {
            // FIXME Perhaps we could move this complexity to RequestTimestamp
            // on server side?
            throw new UnsupportedOperationException("transaction committed under another timestamp");
        }
        logger.info("transaction " + txn.getLocalTimestamp() + " commited globally as " + txn.getGlobalTimestamp());
    }

    /**
     * Requests and assigns a new global timestamp for the transaction.
     * 
     * @param txn
     *            locally committed transaction
     */
    private void requestTxnGlobalTimestamp(AbstractTxnHandle txn) {
        txn.assertStatus(TxnStatus.COMMITTED_LOCAL);

        final AtomicReference<GenerateTimestampReply> timestampReplyRef = new AtomicReference<GenerateTimestampReply>();
        do {
            localEndpoint.send(serverEndpoint, new GenerateTimestampRequest(clientId, txn.getUpdatesDependencyClock(),
                    txn.getGlobalTimestamp()), new GenerateTimestampReplyHandler() {
                @Override
                public void onReceive(RpcConnection conn, GenerateTimestampReply reply) {
                    timestampReplyRef.set(reply);
                }
            }, timeoutMillis);
        } while (timestampReplyRef.get() == null);

        // And replace old timestamp in operations with timestamp from server.
        txn.setGlobalTimestamp(timestampReplyRef.get().getTimestamp());
    }

    /**
     * Applies globally committed transaction locally using a global timestamp.
     * 
     * @param txn
     *            globally committed transaction to apply locally
     */
    private synchronized void applyGloballyCommittedTxn(AbstractTxnHandle txn) {
        txn.assertStatus(TxnStatus.COMMITTED_LOCAL, TxnStatus.COMMITTED_GLOBAL);
        if (txn.getStatus() == TxnStatus.COMMITTED_GLOBAL) {
            return;
        }

        txn.markGloballyCommitted();
        for (final CRDTObjectOperationsGroup opsGroup : txn.getAllGlobalOperations()) {
            // Try to apply changes in a cached copy of an object.
            final CRDT<?> crdt = objectsCache.get(opsGroup.getTargetUID());
            if (crdt == null) {
                logger.warning("object evicted from the local cache before global commit");
            }
            crdt.execute(opsGroup, CRDTOperationDependencyPolicy.IGNORE);
        }
        for (final AbstractTxnHandle dependingTxn : locallyCommittedTxnsQueue) {
            if (dependingTxn != txn) {
                dependingTxn.includeGlobalDependency(txn.getLocalTimestamp(), txn.getGlobalTimestamp());
            }
            // pendingTxn will map timestamp later inside commitToStore().

            // TODO [tricky]: to implement IsolationLevel.READ_COMMITTED we may
            // need
            // to replace timestamps in pending transaction too.
        }
    }

    private synchronized AbstractTxnHandle getNextLocallyCommittedTxnBlocking() {
        while (locallyCommittedTxnsQueue.isEmpty() && !stopFlag) {
            try {
                this.wait();
            } catch (InterruptedException e) {
            }
        }
        return locallyCommittedTxnsQueue.peekFirst();
    }

    private synchronized void setPendingTxn(final AbstractTxnHandle txn) {
        pendingTxn = txn;
        pendingTxnLocalDependencies.clear();
        if (txn != null) {
            pendingTxnLocalDependencies.addAll(locallyCommittedTxnsQueue);
        }
    }

    private void assertNoPendingTransaction() {
        if (pendingTxn != null) {
            throw new IllegalStateException("Only one transaction can be executing at the time");
        }
    }

    private void assertPendingTransaction(final AbstractTxnHandle expectedTxn) {
        if (!pendingTxn.equals(expectedTxn)) {
            throw new IllegalStateException(
                    "Corrupted state: unexpected transaction is bothering me, not the pending one");
        }
    }

    private void assertRunning() {
        if (stopFlag) {
            throw new IllegalStateException("client is stopped");
        }
    }

    /**
     * Thread continuously committing locally committed transactions. The thread
     * takes the oldest locally committed transaction one by one, tries to
     * commit it to the store and applies to it to local cache and depender
     * transactions.
     */
    private class CommitterThread extends Thread {

        public CommitterThread() {
            super("SwiftTransactionCommitterThread");
        }

        @Override
        public void run() {
            while (true) {
                final AbstractTxnHandle nextToCommit = getNextLocallyCommittedTxnBlocking();
                if (stopFlag && (!stopGracefully || nextToCommit == null)) {
                    return;
                }
                commitToStore(nextToCommit);
                applyGloballyCommittedTxn(nextToCommit);
                // Clean up after nextToCommit.
                if (locallyCommittedTxnsQueue.removeFirst() != nextToCommit) {
                    throw new IllegalStateException("internal error, concurrently commiting transactions?");
                }
            }
        }
    }

    private class NotoficationsProcessorThread extends Thread {
        public NotoficationsProcessorThread() {
            super("SwiftNotificationsProcessorThread");
        }

        @Override
        public void run() {
            // FIXME: this is just to test the server!!
            while (true) {
                localEndpoint.send(serverEndpoint, new FastRecentUpdatesRequest(clientId),
                        new FastRecentUpdatesReplyHandler() {
                            @Override
                            public void onReceive(RpcConnection conn, FastRecentUpdatesReply reply) {
                                logger.fine("notifications received for " + reply.getSubscriptions().size()
                                        + " objects");
                            }
                        });
            }
        }
    }

    private static class ObjectUpdatesSubscription {
        private CausalityClock versionRead;
        private boolean notified;
        private AbstractTxnHandle txn;
        private TxnLocalCRDT<?> crdtView;

        public ObjectUpdatesSubscription(CausalityClock version, AbstractTxnHandle txn, TxnLocalCRDT<?> crdtView) {
            this.versionRead = version;
            this.txn = txn;
            this.crdtView = crdtView;
            this.notified = false;
        }
    }
}
