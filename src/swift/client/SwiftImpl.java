package swift.client;

import static sys.net.api.Networking.Networking;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.client.proto.CommitUpdatesReply;
import swift.client.proto.CommitUpdatesReply.CommitStatus;
import swift.client.proto.CommitUpdatesReplyHandler;
import swift.client.proto.CommitUpdatesRequest;
import swift.client.proto.FastRecentUpdatesReply;
import swift.client.proto.FastRecentUpdatesReply.ObjectSubscriptionInfo;
import swift.client.proto.FastRecentUpdatesReply.SubscriptionStatus;
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
import swift.client.proto.UnsubscribeUpdatesRequest;
import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.Timestamp;
import swift.crdt.BaseCRDT;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperationDependencyPolicy;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.ObjectUpdatesListener;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.interfaces.TxnStatus;
import swift.crdt.operations.CRDTObjectOperationsGroup;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcEndpoint;

/**
 * Implementation of Swift client and transactions manager.
 * 
 * @see Swift, TxnManager
 * @author mzawirski
 */
public class SwiftImpl implements Swift, TxnManager {
    // TODO: make sure pruning is supported properly, prune in the cache too?
    // TODO: server failover

    // WISHME: This class uses very coarse-grained locking, but given the
    // complexity of causality tracking and timestamps remapping, unless we
    // prove it is a real issue for a client application, I would rather keep it
    // this way. In any case, locking should not affect responsiveness to
    // pendingTxn requests.

    // WISHME: decouple "object store" from the rest of transactions and
    // notifications processing

    public static final int DEFAULT_TIMEOUT_MILLIS = 10 * 1000;
    public static final int DEFAULT_NOTIFICATION_TIMEOUT_MILLIS = 2 * 60 * 1000;
    public static final long DEFAULT_CACHE_EVICTION_MILLIS = 60 * 1000;
    private static final String CLIENT_CLOCK_ID = "client";
    private static Logger logger = Logger.getLogger(SwiftImpl.class.getName());

    {
        logger.setLevel(Level.WARNING);
    }

    /**
     * Creates new instance of Swift using provided network settings and
     * otherwise default settings.
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
                new TimeBoundedObjectsCache(DEFAULT_CACHE_EVICTION_MILLIS), DEFAULT_TIMEOUT_MILLIS,
                DEFAULT_NOTIFICATION_TIMEOUT_MILLIS);
    }

    /**
     * Creates new instance of Swift using provided network and timeout settings
     * and default cache parameters.
     * 
     * @param localPort
     *            port to bind local RPC endpoint
     * @param serverHostname
     *            hostname of storage server
     * @param serverPort
     *            TCP port of storage server
     * @param timeoutMillis
     *            timeout for server replies in milliseconds
     * @return instance of Swift client
     */
    public static SwiftImpl newInstance(int localPort, String serverHostname, int serverPort, int timeoutMillis) {
        return new SwiftImpl(Networking.rpcBind(localPort, null), Networking.resolve(serverHostname, serverPort),
                new TimeBoundedObjectsCache(DEFAULT_CACHE_EVICTION_MILLIS), timeoutMillis,
                DEFAULT_NOTIFICATION_TIMEOUT_MILLIS);
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

    // Cache of objects.
    // Invariant: if object is in the cache, it must include all updates
    // of locally and globally committed locally-originating transactions.
    private final TimeBoundedObjectsCache objectsCache;

    // Invariant: committedVersion only grows.
    private final CausalityClock committedVersion;
    private IncrementalTimestampGenerator clientTimestampGenerator;

    // Invariant: there is at most one pending (open) transaction.
    private AbstractTxnHandle pendingTxn;
    // Locally committed transactions (in commit order), the first one is
    // possibly committing to the store.
    private final LinkedList<AbstractTxnHandle> locallyCommittedTxnsQueue;
    // Thread sequentially committing transactions from the queue.
    private final CommitterThread committerThread;
    // Local dependencies of a pending transaction.
    private final LinkedList<AbstractTxnHandle> pendingTxnLocalDependencies;

    // Update subscriptions stuff.
    private final Map<CRDTIdentifier, UpdateSubscription> objectUpdateSubscriptions;
    private final NotoficationsProcessorThread notificationsThread;
    private final ExecutorService notificationsCallbacksExecutor;
    private final ExecutorService notificationsSubscriberExecutor;

    private final int timeoutMillis;
    private final int notificationTimeoutMillis;

    SwiftImpl(final RpcEndpoint localEndpoint, final Endpoint serverEndpoint, TimeBoundedObjectsCache objectsCache,
            int timeoutMillis, final int notificationTimeoutMillis) {
        this.clientId = generateClientId();
        this.timeoutMillis = timeoutMillis;
        this.notificationTimeoutMillis = notificationTimeoutMillis;
        this.localEndpoint = localEndpoint;
        this.serverEndpoint = serverEndpoint;
        this.objectsCache = objectsCache;
        this.locallyCommittedTxnsQueue = new LinkedList<AbstractTxnHandle>();
        this.pendingTxnLocalDependencies = new LinkedList<AbstractTxnHandle>();
        this.committedVersion = ClockFactory.newClock();
        this.clientTimestampGenerator = new IncrementalTimestampGenerator(CLIENT_CLOCK_ID);
        this.committerThread = new CommitterThread();
        this.committerThread.start();
        this.objectUpdateSubscriptions = new HashMap<CRDTIdentifier, UpdateSubscription>();
        this.notificationsCallbacksExecutor = Executors.newFixedThreadPool(1);
        this.notificationsSubscriberExecutor = Executors.newFixedThreadPool(1);
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
            // No need to close notifications threads in theory, but it brakes
            // the connection uncleanly.
            notificationsSubscriberExecutor.shutdown();
            notificationsThread.join();
            notificationsCallbacksExecutor.shutdown();
        } catch (InterruptedException e) {
            logger.warning(e.getMessage());
        }
        logger.info("client stopped");
    }

    @Override
    public synchronized AbstractTxnHandle beginTxn(IsolationLevel isolationLevel, CachePolicy cachePolicy,
            boolean readOnly) throws NetworkException {
        // FIXME: Ooops, readOnly is present here at API level, respect it here
        // and in TxnHandleImpl or remove it from API.
        assertNoPendingTransaction();
        assertRunning();

        final Timestamp localTimestamp = clientTimestampGenerator.generateNew();
        switch (isolationLevel) {
        case SNAPSHOT_ISOLATION:
            if (cachePolicy == CachePolicy.MOST_RECENT || cachePolicy == CachePolicy.STRICTLY_MOST_RECENT) {
                final AtomicBoolean doneFlag = new AtomicBoolean(false);
                localEndpoint.send(serverEndpoint, new LatestKnownClockRequest(clientId),
                        new LatestKnownClockReplyHandler() {
                            @Override
                            public void onReceive(RpcConnection conn, LatestKnownClockReply reply) {
                                updateCommittedVersion(reply.getClock());
                                doneFlag.set(true);
                            }
                        }, timeoutMillis);
                if (!doneFlag.get() && cachePolicy == CachePolicy.STRICTLY_MOST_RECENT) {
                    throw new NetworkException("timed out to get transcation snapshot point");
                }
            }
            // Invariant: for SI snapshotClock of a new transaction dominates
            // clock of all previous SI transaction (monotonic reads), since
            // commitedVersion only grows.
            final CausalityClock snapshotClock = committedVersion.clone();
            setPendingTxn(new SnapshotIsolationTxnHandle(this, cachePolicy, localTimestamp, snapshotClock));
            logger.info("SI transaction " + localTimestamp + " started with global snapshot point: " + snapshotClock);
            return pendingTxn;

        case REPEATABLE_READS:
            setPendingTxn(new RepeatableReadsTxnHandle(this, cachePolicy, localTimestamp));
            logger.info("REPEATABLE READS transaction " + localTimestamp + " started");
            return pendingTxn;

        case READ_COMMITTED:
        case READ_UNCOMMITTED:
            // TODO: implement!
        default:
            throw new UnsupportedOperationException("isolation level " + isolationLevel + " unsupported");
        }
    }

    private synchronized void updateCommittedVersion(final CausalityClock clock) {
        if (clock == null) {
            logger.warning("server returned null clock");
            return;
        }

        committedVersion.merge(clock);
        final AbstractTxnHandle commitingTxn = locallyCommittedTxnsQueue.peekFirst();
        final Timestamp commitingTxnTimestamp = commitingTxn == null ? null : commitingTxn.getGlobalTimestamp();
        if (commitingTxnTimestamp != null && clock.includes(commitingTxnTimestamp)) {
            // We observe global visibility (and possibly updates) of locally
            // committed transaction before the CommitUpdatesReply has been
            // received.
            applyGloballyCommittedTxn(commitingTxn);
        }
    }

    @Override
    public synchronized <V extends CRDT<V>> TxnLocalCRDT<V> getObjectLatestVersionTxnView(AbstractTxnHandle txn,
            CRDTIdentifier id, CachePolicy cachePolicy, boolean create, Class<V> classOfV,
            final ObjectUpdatesListener updatesListener) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        assertPendingTransaction(txn);

        TxnLocalCRDT<V> localView;
        if (cachePolicy == CachePolicy.CACHED) {
            localView = getCachedObjectForTxn(id, null, classOfV, updatesListener);
            if (localView != null) {
                return localView;
            }
        }

        // Try to get the latest one.
        final boolean fetchRequired = (cachePolicy != CachePolicy.MOST_RECENT || objectsCache.getAndTouch(id) == null);
        try {
            fetchLatestObject(id, create, classOfV, updatesListener != null, true);
        } catch (VersionNotFoundException x) {
            if (fetchRequired) {
                throw x;
            }
        } catch (NetworkException x) {
            if (fetchRequired) {
                throw x;
            }
        }
        // Pass other exceptions through.

        return getCachedObjectForTxn(id, null, classOfV, updatesListener);
    }

    @Override
    public synchronized <V extends CRDT<V>> TxnLocalCRDT<V> getObjectVersionTxnView(AbstractTxnHandle txn,
            final CRDTIdentifier id, final CausalityClock version, final boolean create, Class<V> classOfV,
            final ObjectUpdatesListener updatesListener) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        assertPendingTransaction(txn);
        assertIsGlobalClock(version);

        TxnLocalCRDT<V> localView = getCachedObjectForTxn(id, version, classOfV, updatesListener);
        if (localView != null) {
            return localView;
        }

        fetchLatestObject(id, create, classOfV, updatesListener != null, true);

        localView = getCachedObjectForTxn(id, version, classOfV, updatesListener);
        if (localView == null) {
            throw new IllegalStateException(
                    "Internal error: just retrieved object unavailable in appropriate version in the cache");
        }
        return localView;
    }

    private CausalityClock clockWithLocallyCommittedDependencies(CausalityClock clock) {
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
    private synchronized <V extends CRDT<V>> TxnLocalCRDT<V> getCachedObjectForTxn(CRDTIdentifier id,
            CausalityClock clock, Class<V> classOfV, ObjectUpdatesListener updatesListener) throws WrongTypeException,
            VersionNotFoundException {
        V crdt;
        try {
            crdt = (V) objectsCache.getAndTouch(id);
        } catch (ClassCastException x) {
            throw new WrongTypeException(x.getMessage());
        }
        if (crdt == null) {
            return null;
        }

        if (clock == null) {
            // Return the latest committed version or, if unavailable, old
            // cached version if it is all committed.
            switch (crdt.getClock().compareTo(committedVersion)) {
            case CMP_ISDOMINATED:
            case CMP_EQUALS:
                clock = crdt.getClock().clone();
                break;
            case CMP_DOMINATES:
                clock = committedVersion.clone();
                break;
            case CMP_CONCURRENT:
                // FIXME: consider intersection of both!
                // need to think a bit if it is safe or not?
                return null;
            default:
                throw new UnsupportedOperationException();
            }

        }
        clock = clockWithLocallyCommittedDependencies(clock);
        final CausalityClock globalClock = clock.clone();
        globalClock.drop(CLIENT_CLOCK_ID);

        final CMP_CLOCK clockCmp = crdt.getClock().compareTo(globalClock);
        if (clockCmp == CMP_CLOCK.CMP_CONCURRENT || clockCmp == CMP_CLOCK.CMP_ISDOMINATED) {
            return null;
        }

        final CMP_CLOCK pruneCmp = globalClock.compareTo(crdt.getPruneClock());
        if (pruneCmp == CMP_CLOCK.CMP_ISDOMINATED || pruneCmp == CMP_CLOCK.CMP_CONCURRENT) {
            throw new VersionNotFoundException("version consistent with current snapshot is not available in the store");
            // TODO: Or just in the cache? return to it if server returns pruned
            // crdt.
        }

        final V crdtReturned;
        // Are there any local dependencies to apply on the cached object?
        if (clock.hasEventFrom(CLIENT_CLOCK_ID)) {
            crdtReturned = crdt.copy();
            // Apply them on sandboxed copy of an object, since these operations
            // use local timestamps.
            for (final AbstractTxnHandle dependentTxn : pendingTxnLocalDependencies) {
                final CRDTObjectOperationsGroup<V> localOps;
                try {
                    localOps = (CRDTObjectOperationsGroup<V>) dependentTxn.getObjectLocalOperations(id);
                } catch (ClassCastException x) {
                    throw new WrongTypeException(x.getMessage());
                }
                if (localOps != null) {
                    crdtReturned.execute(localOps, CRDTOperationDependencyPolicy.IGNORE);
                }
            }
        } else {
            crdtReturned = crdt;
        }
        final TxnLocalCRDT<V> crdtView = crdtReturned.getTxnLocalCopy(clock, pendingTxn);
        if (updatesListener != null) {
            final UpdateSubscription subscription = addUpdateSubscription(crdtReturned, crdtView, updatesListener);
            if (subscription.hasListener() && crdtReturned.hasUpdatesSince(clock)) {
                notificationsCallbacksExecutor.execute(subscription.generateListenerNotification(id));
            }
        }
        return crdtView;
    }

    private <V extends CRDT<V>> void fetchLatestObject(CRDTIdentifier id, boolean create, Class<V> classOfV,
            final boolean subscribeUpdates, final boolean clientTriggered) throws WrongTypeException,
            NoSuchObjectException, VersionNotFoundException, NetworkException {
        final V crdt;
        final CausalityClock minVersion;
        synchronized (this) {
            try {
                crdt = (V) objectsCache.getWithoutTouch(id);
            } catch (ClassCastException x) {
                throw new WrongTypeException(x.getMessage());
            }
            minVersion = clockWithLocallyCommittedDependencies(committedVersion);
            minVersion.drop(CLIENT_CLOCK_ID);
        }

        if (crdt == null) {
            fetchLatestObjectFromScratch(id, create, classOfV, minVersion, subscribeUpdates, clientTriggered);
        } else {
            fetchLatestObjectByRefresh(id, create, classOfV, crdt, minVersion, subscribeUpdates, clientTriggered);
        }
    }

    @SuppressWarnings("unchecked")
    private <V extends CRDT<V>> void fetchLatestObjectFromScratch(CRDTIdentifier id, boolean create, Class<V> classOfV,
            CausalityClock minVersion, boolean subscribeUpdates, boolean clientTriggered) throws NoSuchObjectException,
            WrongTypeException, VersionNotFoundException, NetworkException {
        final AtomicReference<FetchObjectVersionReply> replyRef = new AtomicReference<FetchObjectVersionReply>();
        final SubscriptionType subscriptionType = subscribeUpdates ? SubscriptionType.UPDATES : SubscriptionType.NONE;
        localEndpoint.send(serverEndpoint, new FetchObjectVersionRequest(clientId, id, minVersion, subscriptionType),
                new FetchObjectVersionReplyHandler() {
                    @Override
                    public void onReceive(RpcConnection conn, FetchObjectVersionReply reply) {
                        replyRef.set(reply);
                    }
                }, timeoutMillis);
        if (replyRef.get() == null) {
            throw new NetworkException("Fetching object version timed out");
        }
        processFetchObjectReply(id, create, classOfV, replyRef.get(), subscribeUpdates, clientTriggered);
    }

    @SuppressWarnings("unchecked")
    private <V extends CRDT<V>> void fetchLatestObjectByRefresh(CRDTIdentifier id, boolean create, Class<V> classOfV,
            V cachedCrdt, CausalityClock minVersion, boolean subscribeUpdates, boolean clientTriggered)
            throws NoSuchObjectException, WrongTypeException, VersionNotFoundException, NetworkException {
        final CausalityClock oldCrdtClock;
        synchronized (this) {
            oldCrdtClock = cachedCrdt.getClock().clone();
        }

        // WISHME: we should replace it with deltas or operations list
        final AtomicReference<FetchObjectVersionReply> replyRef = new AtomicReference<FetchObjectVersionReply>();
        final SubscriptionType subscriptionType = subscribeUpdates ? SubscriptionType.UPDATES : SubscriptionType.NONE;
        localEndpoint.send(serverEndpoint, new FetchObjectDeltaRequest(clientId, id, oldCrdtClock, minVersion,
                subscriptionType), new FetchObjectVersionReplyHandler() {
            @Override
            public void onReceive(RpcConnection conn, FetchObjectVersionReply reply) {
                replyRef.set(reply);
            }
        }, timeoutMillis);
        if (replyRef.get() == null) {
            throw new NetworkException("Fetching newer object version timed out");
        }
        processFetchObjectReply(id, create, classOfV, replyRef.get(), subscribeUpdates, clientTriggered);
    }

    private <V extends CRDT<V>> void processFetchObjectReply(CRDTIdentifier id, boolean create, Class<V> classOfV,
            final FetchObjectVersionReply fetchReply, boolean requestedSubscribeUpdates, boolean clientTriggered)
            throws NoSuchObjectException, WrongTypeException, VersionNotFoundException {
        final V crdt;
        switch (fetchReply.getStatus()) {
        case OBJECT_NOT_FOUND:
            if (!create) {
                throw new NoSuchObjectException("object " + id + " not found");
            }
            try {
                crdt = classOfV.newInstance();
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }
            crdt.init(id, fetchReply.getVersion(), fetchReply.getPruneClock(), false);
            break;
        case VERSION_NOT_FOUND:
        case OK:
            try {
                crdt = (V) fetchReply.getCrdt();
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }
            crdt.init(id, fetchReply.getVersion(), fetchReply.getPruneClock(), true);
            break;
        default:
            throw new IllegalStateException("Unexpected status code" + fetchReply.getStatus());
        }

        synchronized (this) {
            updateCommittedVersion(fetchReply.getEstimatedLatestKnownClock());

            final V cacheCRDT;
            try {
                if (clientTriggered) {
                    cacheCRDT = (V) objectsCache.getAndTouch(id);
                } else {
                    cacheCRDT = (V) objectsCache.getWithoutTouch(id);
                }
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }

            if (cacheCRDT == null) {
                objectsCache.add(crdt);
            } else {
                cacheCRDT.merge(crdt);

                UpdateSubscription subscription = objectUpdateSubscriptions.get(id);
                if (requestedSubscribeUpdates && subscription == null) {
                    // Add temporary subscription entry without specifying full
                    // information on what value has been read.
                    subscription = addUpdateSubscription(cacheCRDT, null, null);
                }

                if (subscription != null && subscription.hasListener()
                        && cacheCRDT.hasUpdatesSince(subscription.readVersion)) {
                    notificationsCallbacksExecutor.execute(subscription.generateListenerNotification(id));
                }
            }
        }

        if (fetchReply.getStatus() == FetchStatus.VERSION_NOT_FOUND) {
            // TODO: retry if version <= minVerson?
            throw new VersionNotFoundException("requested version not found in the store");
        }
    }

    private void fetchSubscribedNotifications() {
        final AtomicReference<FastRecentUpdatesReply> replyRef = new AtomicReference<FastRecentUpdatesReply>();
        localEndpoint.send(serverEndpoint,
                new FastRecentUpdatesRequest(clientId, Math.max(0, notificationTimeoutMillis - timeoutMillis)),
                new FastRecentUpdatesReplyHandler() {
                    @Override
                    public void onReceive(RpcConnection conn, FastRecentUpdatesReply reply) {
                        replyRef.set(reply);
                    }
                }, notificationTimeoutMillis);
        final FastRecentUpdatesReply notifications = replyRef.get();
        if (notifications == null) {
            logger.warning("server timed out on subscriptions information request");
            return;
        }

        logger.fine("notifications received for " + notifications.getSubscriptions().size() + " objects");
        updateCommittedVersion(notifications.getEstimatedLatestKnownClock());
        if (notifications.getStatus() == SubscriptionStatus.ACTIVE) {
            // Process notifications.
            for (final ObjectSubscriptionInfo subscriptionInfo : notifications.getSubscriptions()) {
                if (subscriptionInfo.isDirty() && subscriptionInfo.getUpdates().isEmpty()) {
                    logger.warning("unexpected server notification information without update");
                } else {
                    applyObjectUpdates(subscriptionInfo.getId(), subscriptionInfo.getOldClock(),
                            subscriptionInfo.getUpdates(), subscriptionInfo.getNewClock());
                }
            }
        } else {
            // Renew lost subscriptions.
            synchronized (this) {
                for (final CRDTIdentifier id : objectUpdateSubscriptions.keySet()) {
                    asyncSubscribeObjectUpdates(id);
                }
            }
        }
    }

    /**
     * @return true if subscription should be continued for this object
     */
    private synchronized void applyObjectUpdates(final CRDTIdentifier id, final CausalityClock dependencyClock,
            final List<CRDTObjectOperationsGroup<?>> ops, final CausalityClock outputClock) {
        final UpdateSubscription subscription = objectUpdateSubscriptions.get(id);
        if (subscription == null) {
            removeUpdateSubscriptionAsyncUnsubscribe(id);
        }

        final CRDT crdt = objectsCache.getWithoutTouch(id);
        if (crdt == null) {
            // Ooops, we evicted the object from the cache.
            logger.info("cannot apply received updates on object " + id + " as it has been evicted from the cache");
            if (subscription != null) {
                if (subscription.hasListener()) {
                    // There still listener waiting, make some efforts to fire
                    // the notification.
                    asyncSubscribeObjectUpdates(id);
                } else {
                    // Stop subscription for object evicted from the cache.
                    removeUpdateSubscriptionAsyncUnsubscribe(id);
                }
            }
            return;
        }

        final CMP_CLOCK clkCmp = crdt.getClock().compareTo(dependencyClock);
        if (clkCmp == CMP_CLOCK.CMP_ISDOMINATED || clkCmp == CMP_CLOCK.CMP_CONCURRENT) {
            // Ooops, we missed some update or messages were ordered.
            logger.info("cannot apply received updates on object " + id + " due to unsatisfied dependencies");
            if (subscription != null) {
                asyncSubscribeObjectUpdates(id);
            }
            return;
        }

        for (final CRDTObjectOperationsGroup<?> op : ops) {
            if (!crdt.execute(op, CRDTOperationDependencyPolicy.RECORD_BLINDLY)) {
                // Already applied update.
                continue;
            }
            if (subscription != null && subscription.hasListener()) {
                if (subscription.readVersion.includes(op.getBaseTimestamp())) {
                    logger.warning("Client invariant broken: applied unknown operation, yet previously read");
                } else {
                    notificationsCallbacksExecutor.execute(subscription.generateListenerNotification(id));
                }
            }
        }
        crdt.getClock().merge(outputClock);
    }

    private synchronized UpdateSubscription addUpdateSubscription(final CRDT<?> crdt, final TxnLocalCRDT<?> localView,
            ObjectUpdatesListener listener) {
        final UpdateSubscription updateSubscription = new UpdateSubscription(pendingTxn, localView, listener);
        // Overwriting old entry and even subscribing again is fine, the
        // interface specifies clearly that the latest get() matters.
        final UpdateSubscription oldSubscription = objectUpdateSubscriptions.put(crdt.getUID(), updateSubscription);
        if (oldSubscription == null && crdt.isRegisteredInStore()) {
            // If object is not in the store yet, wait until
            // applyGloballyCommittedTxn() with actual subscription.
            asyncSubscribeObjectUpdates(crdt.getUID());
        }
        return updateSubscription;
    }

    private void asyncSubscribeObjectUpdates(final CRDTIdentifier id) {
        notificationsSubscriberExecutor.execute(new Runnable() {
            @Override
            public void run() {
                if (!objectUpdateSubscriptions.containsKey(id)) {
                    return;
                }
                try {
                    fetchLatestObject(id, false, BaseCRDT.class, true, false);
                } catch (SwiftException x) {
                    logger.warning("could not fetch the latest version of an object for notifications purposes: "
                            + x.getMessage());
                }
            }
        });
    }

    private synchronized void removeUpdateSubscriptionAsyncUnsubscribe(final CRDTIdentifier id) {
        objectUpdateSubscriptions.remove(id);
        notificationsSubscriberExecutor.execute(new Runnable() {
            @Override
            public void run() {
                if (objectUpdateSubscriptions.containsKey(id)) {
                    return;
                }
                if (!localEndpoint.send(serverEndpoint, new UnsubscribeUpdatesRequest(clientId, id))) {
                    logger.warning("could not unsuscribe object updates");
                }
            }
        });
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
        objectsCache.evictOutdated();
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
        if (txn.getUpdatesDependencyClock().hasEventFrom(CLIENT_CLOCK_ID)) {
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
            final CRDT<?> crdt = objectsCache.getWithoutTouch(opsGroup.getTargetUID());
            if (crdt == null) {
                logger.warning("object evicted from the local cache before global commit");
            } else {
                crdt.execute(opsGroup, CRDTOperationDependencyPolicy.IGNORE);
                // Subscribe updates if they were requested.
                final UpdateSubscription subscription = objectUpdateSubscriptions.get(opsGroup.getTargetUID());
                if (subscription != null && opsGroup.hasCreationState()) {
                    asyncSubscribeObjectUpdates(opsGroup.getTargetUID());
                }
            }
        }
        objectsCache.recordOnAll(txn.getGlobalTimestamp());

        for (final AbstractTxnHandle dependingTxn : locallyCommittedTxnsQueue) {
            if (dependingTxn != txn) {
                dependingTxn.includeGlobalDependency(txn.getLocalTimestamp(), txn.getGlobalTimestamp());
            }
            // pendingTxn will map timestamp later inside commitToStore().

            // TODO [tricky]: to implement IsolationLevel.READ_COMMITTED we may
            // need to replace timestamps in pending transaction too.
        }
        for (final UpdateSubscription subscription : objectUpdateSubscriptions.values()) {
            subscription.replaceReadVersionTimestamp(txn.getLocalTimestamp(), txn.getGlobalTimestamp());
        }
        committedVersion.record(txn.getGlobalTimestamp());
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

    private void notifyUpdatesListener(final ObjectUpdatesListener listener, final TxnHandle txn,
            final CRDTIdentifier id, final TxnLocalCRDT<?> crdtView) {
        notificationsCallbacksExecutor.execute(new Runnable() {
            @Override
            public void run() {
                listener.onObjectUpdate(txn, id, crdtView);
            }
        });
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

    private void assertIsGlobalClock(CausalityClock version) {
        if (version.hasEventFrom(CLIENT_CLOCK_ID)) {
            throw new IllegalArgumentException("transaction requested visibility of local transaction");
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
            while (true) {
                synchronized (SwiftImpl.this) {
                    if (stopFlag) {
                        return;
                    }
                }
                fetchSubscribedNotifications();
            }
        }
    }

    /**
     * Client representation of updates subscription. When listener is not null,
     * the listener is awaiting for notification on update that occurred after
     * the readVersion.
     * 
     * @author mzawirski
     */
    private static class UpdateSubscription {
        public ObjectUpdatesListener listener;
        private AbstractTxnHandle txn;
        private TxnLocalCRDT<?> crdtView;
        private CausalityClock readVersion;

        public UpdateSubscription(AbstractTxnHandle txn, TxnLocalCRDT<?> crdtView, final ObjectUpdatesListener listener) {
            // This comparison is a bit hackish, perhaps we should support it
            // better at API level.
            if (listener != TxnHandle.DUMMY_UPDATES_SUBSCRIBER) {
                this.txn = txn;
                this.crdtView = crdtView;
                this.listener = listener;
                this.readVersion = crdtView.getClock().clone();
            }
            // else: only subscribe updates, but do not bother with notifying on
            // updates
        }

        public boolean hasListener() {
            return listener != null;
        }

        // FIXME: it seems more reasonable to call it after we make sure the
        // triggering update is included in committedVersion!
        public Runnable generateListenerNotification(final CRDTIdentifier id) {
            if (!hasListener()) {
                return null;
            }

            final ObjectUpdatesListener listenerRef = this.listener;
            final AbstractTxnHandle txnRef = this.txn;
            final TxnLocalCRDT<?> crdtRef = this.crdtView;
            txn = null;
            crdtView = null;
            listener = null;
            readVersion = null;

            return new Runnable() {
                @Override
                public void run() {
                    listenerRef.onObjectUpdate(txnRef, id, crdtRef);
                }
            };
        }

        public void replaceReadVersionTimestamp(Timestamp localTimestamp, Timestamp globalTimestamp) {
            if (!hasListener()) {
                return;
            }

            // Objects in cache always use global timestamp, so we need to map
            // stuff.
            if (readVersion.includes(localTimestamp)) {
                readVersion.drop(localTimestamp);
                readVersion.record(globalTimestamp);
            }
        }
    }
}
