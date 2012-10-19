package swift.client;

import static sys.net.api.Networking.Networking;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import swift.client.proto.BatchCommitUpdatesReply;
import swift.client.proto.BatchCommitUpdatesReplyHandler;
import swift.client.proto.BatchCommitUpdatesRequest;
import swift.client.proto.CommitUpdatesReply;
import swift.client.proto.CommitUpdatesRequest;
import swift.client.proto.FastRecentUpdatesReply;
import swift.client.proto.FastRecentUpdatesReply.ObjectSubscriptionInfo;
import swift.client.proto.FastRecentUpdatesReply.SubscriptionStatus;
import swift.client.proto.FastRecentUpdatesReplyHandler;
import swift.client.proto.FastRecentUpdatesRequest;
import swift.client.proto.FetchObjectVersionReply;
import swift.client.proto.FetchObjectVersionReply.FetchStatus;
import swift.client.proto.FetchObjectVersionReplyHandler;
import swift.client.proto.FetchObjectVersionRequest;
import swift.client.proto.LatestKnownClockReply;
import swift.client.proto.LatestKnownClockReplyHandler;
import swift.client.proto.LatestKnownClockRequest;
import swift.client.proto.SubscriptionType;
import swift.client.proto.UnsubscribeUpdatesRequest;
import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.ReturnableTimestampSourceDecorator;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.crdt.BaseCRDT;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperationDependencyPolicy;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.ObjectUpdatesListener;
import swift.crdt.interfaces.SwiftScout;
import swift.crdt.interfaces.SwiftSession;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.interfaces.TxnStatus;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.utils.CallableWithDeadline;
import swift.utils.DummyLog;
import swift.utils.ExponentialBackoffTaskExecutor;
import swift.utils.KryoDiskLog;
import swift.utils.NoFlushLogDecorator;
import swift.utils.TransactionsLog;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;

/**
 * Implementation of Swift scout and transactions manager. Scout can either
 * support one or multiple client sessions.
 * 
 * @see Swift, TxnManager
 * @author mzawirski
 */
public class SwiftImpl implements SwiftScout, TxnManager {
    // Temporary Nuno's hack:
    public static final boolean DEFAULT_LISTENER_FOR_GET = false;
    static ObjectUpdatesListener DEFAULT_LISTENER = new AbstractObjectUpdatesListener() {
        public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, TxnLocalCRDT<?> previousValue) {
            // do nothing
        };
    };

    // TODO: server failover

    // WISHME: notifications are quite CPU/memory scans-intensive at the
    // moment; consider more efficient implementation if this is an issue
    // (especially for scenarios where dummy TxnHandle.UPDATES_SUBSCRIBER is
    // employed).

    // WISHME: This class uses very coarse-grained locking, but given the
    // complexity of causality tracking and timestamps remapping, unless we
    // prove it is a real issue for a client application, I would rather keep it
    // this way. In any case, locking should not affect responsiveness to
    // pendingTxn requests.

    // WISHME: split this monolithic untestable monster, e.g. extract
    // "object store" from the rest of transactions and notifications
    // processing.

    // WISHME: subscribe updates of frequently accessed objects

    // The two below yield the following sequence of wait times: 10, 20, 40...
    public static final int RPC_RETRY_WAIT_TIME_MULTIPLIER = 2;
    public static final int INIT_RPC_RETRY_WAIT_TIME_MILLIS = 10;

    private static Logger logger = Logger.getLogger(SwiftImpl.class.getName());

    /**
     * Creates new single session instance backed by a scout.
     * 
     * @param options
     *            Swift scout options
     * @return instance of Swift client session
     * @see SwiftOptions
     */
    public static SwiftSession newSingleSessionInstance(final SwiftOptions options) {
        final SwiftScout sharedImpl = new SwiftImpl(Networking.rpcConnect().toDefaultService(), Networking.resolve(
                options.getServerHostname(), options.getServerPort()), new TimeSizeBoundedObjectsCache(
                options.getCacheEvictionTimeMillis(), options.getCacheSize()), options);
        return sharedImpl.newSession("singleton-session");
    }

    /**
     * Creates a new scout that allows many open sessions.
     * 
     * @param options
     *            Swift scout options
     * @return instance of Swift client session
     * @see SwiftOptions
     */
    public static SwiftScout newMultiSessionInstance(final SwiftOptions options) {
        return new SwiftImpl(Networking.rpcConnect().toDefaultService(), Networking.resolve(
                options.getServerHostname(), options.getServerPort()), new TimeSizeBoundedObjectsCache(
                options.getCacheEvictionTimeMillis(), options.getCacheSize()), options);
    }

    private static String generateScoutId() {
        final UUID uuid = UUID.randomUUID();
        final byte[] uuidBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE * 2).putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits()).array();
        return DatatypeConverter.printBase64Binary(uuidBytes);
    }

    private boolean stopFlag;
    private boolean stopGracefully;

    private final String scoutId;
    private final RpcEndpoint localEndpoint;
    private final Endpoint serverEndpoint;

    // Cache of objects.
    // Invariant: if object is in the cache, it includes all updates of locally
    // and globally committed locally-originating transactions.
    private final TimeSizeBoundedObjectsCache objectsCache;

    // CLOCKS: all clocks grow over time. Careful with references, use copies.

    // A clock known to be committed at the store.
    private final CausalityClock committedVersion;
    // A clock known to be committed at the store and eventually observable
    // across the system even in case of disaster affecting part of the store.
    private final CausalityClock committedDisasterDurableVersion;
    // Last locally committed txn clock + dependencies.
    private CausalityClock lastLocallyCommittedTxnClock;
    // Last globally committed txn clock + dependencies.
    private CausalityClock lastGloballyCommittedTxnClock;
    // Generator of local timestamps.
    private final ReturnableTimestampSourceDecorator<Timestamp> clientTimestampGenerator;

    // Set of versions for fetch requests in progress.
    private final Set<CausalityClock> fetchVersionsInProgress;

    private Set<AbstractTxnHandle> pendingTxns;
    // Locally committed transactions (in begin-txn order), the first one is
    // possibly committing to the store.
    private final SortedSet<AbstractTxnHandle> locallyCommittedTxnsOrderedQueue;
    // Globally committed local transactions (in commit order), but possibly not
    // stable, i.e. not distaster-safe in the store.
    private final LinkedList<AbstractTxnHandle> globallyCommittedUnstableTxns;

    // Thread sequentially committing transactions from the queue.
    private final CommitterThread committerThread;

    // Update subscriptions stuff.
    // id -> sessionId -> update subscription information; the presence of any
    // first-level mapping corresponds roughly to the fact that
    // updates are currently subscribed; if there is any 2nd level mapping, it
    // means that a listener is additionally installed and awaits notification
    private final Map<CRDTIdentifier, Map<String, UpdateSubscriptionWithListener>> objectSessionsUpdateSubscriptions;
    // map from timestamp mapping of an uncommitted update to objects that may
    // await notification with this mapping
    private final Map<TimestampMapping, Set<CRDTIdentifier>> uncommittedUpdatesObjectsToNotify;
    private final NotoficationsProcessorThread notificationsThread;
    private final ExecutorService notificationsCallbacksExecutor;
    private final ExecutorService notificationsSubscriberExecutor;
    private final ExponentialBackoffTaskExecutor retryableTaskExecutor;

    private final TransactionsLog durableLog;
    private final CacheStats cacheStats;

    // OPTIONS
    private final int timeoutMillis;
    private final int notificationTimeoutMillis;
    private final int deadlineMillis;
    // If true, only disaster safe committed (and local) transactions are read
    // by transactions, so the scout virtually never blocks due to systen
    // failures.
    private final boolean disasterSafe;
    // If true, multiple transactions can be open. Note that (1) transactions
    // are always committed in the order of beginTxn() calls, not commit(), and
    // (2) with the enabled option, all update transactions need to be committed
    // even if they made no updates or rolled back.
    private final boolean concurrentOpenTransactions;
    // Maximum number of asynchronous transactions queued locally before they
    // block the application.
    private final int maxAsyncTransactionsQueued;
    // Maximum number of transactions in a single commit request to the store.
    private final int maxCommitBatchSize;

    SwiftImpl(final RpcEndpoint localEndpoint, final Endpoint serverEndpoint,
            final TimeSizeBoundedObjectsCache objectsCache, final SwiftOptions options) {
        this.scoutId = generateScoutId();
        this.concurrentOpenTransactions = options.isConcurrentOpenTransactions();
        this.maxAsyncTransactionsQueued = options.getMaxAsyncTransactionsQueued();
        this.disasterSafe = options.isDisasterSafe();
        this.timeoutMillis = options.getTimeoutMillis();
        this.deadlineMillis = options.getDeadlineMillis();
        this.notificationTimeoutMillis = options.getNotificationTimeoutMillis();
        this.maxCommitBatchSize = options.getMaxCommitBatchSize();
        this.localEndpoint = localEndpoint;
        this.serverEndpoint = serverEndpoint;
        this.objectsCache = objectsCache;
        this.cacheStats = new CacheStats();

        this.locallyCommittedTxnsOrderedQueue = new TreeSet<AbstractTxnHandle>();
        this.globallyCommittedUnstableTxns = new LinkedList<AbstractTxnHandle>();
        this.lastLocallyCommittedTxnClock = ClockFactory.newClock();
        this.lastGloballyCommittedTxnClock = ClockFactory.newClock();
        this.committedDisasterDurableVersion = ClockFactory.newClock();
        this.committedVersion = ClockFactory.newClock();
        this.clientTimestampGenerator = new ReturnableTimestampSourceDecorator<Timestamp>(
                new IncrementalTimestampGenerator(scoutId));
        this.retryableTaskExecutor = new ExponentialBackoffTaskExecutor("client->server request",
                INIT_RPC_RETRY_WAIT_TIME_MILLIS, RPC_RETRY_WAIT_TIME_MULTIPLIER);
        this.pendingTxns = new HashSet<AbstractTxnHandle>();
        this.committerThread = new CommitterThread();
        this.fetchVersionsInProgress = new HashSet<CausalityClock>();
        this.committerThread.start();
        this.objectSessionsUpdateSubscriptions = new HashMap<CRDTIdentifier, Map<String, UpdateSubscriptionWithListener>>();
        this.uncommittedUpdatesObjectsToNotify = new HashMap<TimestampMapping, Set<CRDTIdentifier>>();
        this.notificationsCallbacksExecutor = Executors.newFixedThreadPool(options.getNotificationThreadPoolsSize());
        this.notificationsSubscriberExecutor = Executors.newFixedThreadPool(options.getNotificationThreadPoolsSize());
        this.notificationsThread = new NotoficationsProcessorThread();
        this.notificationsThread.start();

        TransactionsLog log = new DummyLog();
        if (options.getLogFilename() != null) {
            try {
                log = new KryoDiskLog(options.getLogFilename());
            } catch (FileNotFoundException x) {
                // TODO: Propagate the exception
                logger.warning("Could not create a log file " + options.getLogFilename() + " (using no log instead): "
                        + x);
            }
        }
        if (!options.isLogFlushOnCommit()) {
            log = new NoFlushLogDecorator(log);
        }
        this.durableLog = log;
    }

    public void stop(boolean waitForCommit) {
        logger.info("Stopping scout");
        synchronized (this) {
            if (stopFlag) {
                logger.warning("Scout is already stopped");
                return;
            }
            if (!pendingTxns.isEmpty()) {
                logger.warning("Stopping while there are pending transactions!");
                return;
            }

            stopFlag = true;
            stopGracefully = waitForCommit;
            this.notifyAll();
        }
        try {
            committerThread.join();
            for (final CRDTIdentifier id : new ArrayList<CRDTIdentifier>(objectSessionsUpdateSubscriptions.keySet())) {
                removeUpdateSubscriptionAsyncUnsubscribe(id);
            }
            notificationsSubscriberExecutor.shutdown();
            notificationsCallbacksExecutor.shutdown();
            notificationsSubscriberExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            notificationsCallbacksExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            durableLog.close();
            // Do not wait for notifications thread, as it is hard to interrupt
            // pending notification reply.
            // if (stopGracefully) {
            // notificationsThread.join();
            // }
        } catch (InterruptedException e) {
            logger.warning(e.getMessage());
        }
        logger.info("scout stopped");
        cacheStats.printAndReset();
    }

    @Override
    public SwiftSession newSession(String sessionId) {
        return new SwiftSessionToScoutAdapter(this, sessionId);
    }

    @Override
    public void printAndResetCacheStats() {
        cacheStats.printAndReset();
    }

    public synchronized AbstractTxnHandle beginTxn(String sessionId, IsolationLevel isolationLevel,
            CachePolicy cachePolicy, boolean readOnly) throws NetworkException {
        if (!concurrentOpenTransactions && !pendingTxns.isEmpty()) {
            throw new IllegalStateException("Only one transaction can be executing at the time");
        }
        assertRunning();

        switch (isolationLevel) {
        case SNAPSHOT_ISOLATION:
            if (cachePolicy == CachePolicy.MOST_RECENT || cachePolicy == CachePolicy.STRICTLY_MOST_RECENT) {
                final Boolean reply = retryableTaskExecutor.execute(new CallableWithDeadline<Boolean>(false,
                        deadlineMillis) {

                    public String toString() {
                        return "LatestKnownClockRequest";
                    }

                    @Override
                    protected Boolean callOrFailWithNull() {
                        localEndpoint.send(serverEndpoint, new LatestKnownClockRequest(scoutId),
                                new LatestKnownClockReplyHandler() {
                                    @Override
                                    public void onReceive(RpcHandle conn, LatestKnownClockReply reply) {
                                        updateCommittedVersions(reply.getClock(), reply.getDistasterDurableClock());
                                        setResult(true);
                                    }
                                }, Math.min(timeoutMillis, getDeadlineLeft()));

                        return super.getResult();
                    }
                });

                if (reply == null && cachePolicy == CachePolicy.STRICTLY_MOST_RECENT) {
                    throw new NetworkException("timed out to get transcation snapshot point");
                }
            }
            // Invariant: for SI snapshotClock of a new transaction dominates
            // clock of all previous SI transaction (monotonic reads), since
            // commitedVersion only grows.
            final CausalityClock snapshotClock = getCommittedVersion(true);
            snapshotClock.merge(lastLocallyCommittedTxnClock);
            final SnapshotIsolationTxnHandle siTxn;
            if (readOnly) {
                siTxn = new SnapshotIsolationTxnHandle(this, sessionId, cachePolicy, snapshotClock);
            } else {
                final TimestampMapping timestampMapping = generateNextTimestampMapping();
                siTxn = new SnapshotIsolationTxnHandle(this, sessionId, durableLog, cachePolicy, timestampMapping,
                        snapshotClock);
            }
            addPendingTxn(siTxn);
            if (logger.isLoggable(Level.INFO)) {
                logger.info("SI " + siTxn + " started with snapshot point: " + snapshotClock);
            }
            return siTxn;

        case REPEATABLE_READS:
            final RepeatableReadsTxnHandle rrTxn;
            if (readOnly) {
                rrTxn = new RepeatableReadsTxnHandle(this, sessionId, cachePolicy);
            } else {
                final TimestampMapping timestampMapping = generateNextTimestampMapping();
                rrTxn = new RepeatableReadsTxnHandle(this, sessionId, durableLog, cachePolicy, timestampMapping);
            }
            addPendingTxn(rrTxn);
            if (logger.isLoggable(Level.INFO)) {
                logger.info("REPEATABLE READS  " + rrTxn + " started");
            }
            return rrTxn;

        case READ_COMMITTED:
        case READ_UNCOMMITTED:
            // TODO: implement!
        default:
            throw new UnsupportedOperationException("isolation level " + isolationLevel + " unsupported");
        }
    }

    private TimestampMapping generateNextTimestampMapping() {
        return new TimestampMapping(clientTimestampGenerator.generateNew());
    }

    private void returnLastTimestamp() {
        // Return and reuse last timestamp to avoid holes in VV.
        clientTimestampGenerator.returnLastTimestamp();
    }

    private synchronized void updateCommittedVersions(final CausalityClock newCommittedVersion,
            final CausalityClock newCommittedDisasterDurableVersion) {
        if (newCommittedVersion == null || newCommittedDisasterDurableVersion == null) {
            logger.warning("server returned null clock");
            return;
        }

        boolean committedVersionUpdated = this.committedVersion.merge(newCommittedVersion).is(CMP_CLOCK.CMP_DOMINATES,
                CMP_CLOCK.CMP_EQUALS);
        boolean committedDisasterDurableUpdated = this.committedDisasterDurableVersion.merge(
                newCommittedDisasterDurableVersion).is(CMP_CLOCK.CMP_DOMINATES, CMP_CLOCK.CMP_EQUALS);
        if (committedVersionUpdated && committedDisasterDurableUpdated) {
            // No changes.
            return;
        }

        // Find and clean new stable local txns logs that we won't need anymore.
        int stableTxnsToDiscard = 0;
        int evaluatedTxns = 0;
        for (final AbstractTxnHandle txn : globallyCommittedUnstableTxns) {
            final TimestampMapping txnMapping = txn.getTimestampMapping();
            if (txnMapping.hasSystemTimestamp()) {
                boolean notNeeded = txnMapping.allSystemTimestampsIncluded(committedDisasterDurableVersion);
                for (final CausalityClock fetchedVersion : fetchVersionsInProgress) {
                    if (!txnMapping.allSystemTimestampsIncluded(fetchedVersion)) {
                        notNeeded = false;
                        break;
                    }
                }
                if (notNeeded) {
                    stableTxnsToDiscard = evaluatedTxns + 1;
                } else {
                    break;
                }
            } else {
                // The txn has unknown system timestamp, so we need to rely on
                // subsequent transactions to determine if it can be removed.
            }
            evaluatedTxns++;
        }
        for (int i = 0; i < stableTxnsToDiscard; i++) {
            globallyCommittedUnstableTxns.removeFirst();
        }

        // Go through updates to notify and see if any become committed.
        final Iterator<Entry<TimestampMapping, Set<CRDTIdentifier>>> iter = uncommittedUpdatesObjectsToNotify
                .entrySet().iterator();
        while (iter.hasNext()) {
            final Entry<TimestampMapping, Set<CRDTIdentifier>> entry = iter.next();
            if (entry.getKey().anyTimestampIncluded(getCommittedVersion(false))) {
                iter.remove();
                for (final CRDTIdentifier id : entry.getValue()) {
                    final Map<String, UpdateSubscriptionWithListener> subscriptions = objectSessionsUpdateSubscriptions
                            .get(id);
                    if (subscriptions != null) {
                        for (UpdateSubscriptionWithListener subscription : subscriptions.values()) {
                            notificationsCallbacksExecutor.execute(subscription
                                    .generateNotificationAndDiscard(this, id));
                        }
                    }
                }
            }
        }
    }

    private synchronized CausalityClock getCommittedVersion(boolean copy) {
        CausalityClock result;
        if (disasterSafe) {
            result = committedDisasterDurableVersion;
        } else {
            result = committedVersion;
        }

        if (copy) {
            result = result.clone();
        }
        return result;
    }

    @Override
    public <V extends CRDT<V>> TxnLocalCRDT<V> getObjectLatestVersionTxnView(AbstractTxnHandle txn, CRDTIdentifier id,
            CachePolicy cachePolicy, boolean create, Class<V> classOfV, final ObjectUpdatesListener updatesListener)
            throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
        synchronized (this) {
            assertPendingTransaction(txn);

            if (cachePolicy == CachePolicy.CACHED) {
                try {
                    final TxnLocalCRDT<V> view = getCachedObjectForTxn(txn, id, null, classOfV, updatesListener, false);
                    cacheStats.addCacheHit(id);
                    return view;
                } catch (NoSuchObjectException x) {
                    cacheStats.addCacheMissNoObject(id);
                    // Ok, let's try to fetch then.
                } catch (VersionNotFoundException x) {
                    cacheStats.addCacheMissBizarre(id);
                    logger.warning("No self-consistent version found in cache: " + x);
                }
            }
        }

        while (true) {
            final CausalityClock fetchClock;
            final boolean fetchStrictlyRequired;
            synchronized (this) {
                fetchStrictlyRequired = (cachePolicy == CachePolicy.STRICTLY_MOST_RECENT || objectsCache
                        .getAndTouch(id) == null);
                fetchClock = getCommittedVersion(true);
                fetchClock.merge(lastGloballyCommittedTxnClock);
                fetchClock.drop(scoutId);
                // Try to get the latest one.
            }

            boolean fetchError = false;
            try {
                fetchObjectVersion(txn, id, create, classOfV, fetchClock, false, updatesListener != null);
            } catch (VersionNotFoundException x) {
                if (fetchStrictlyRequired) {
                    throw x;
                }
                fetchError = true;
            } catch (NetworkException x) {
                if (fetchStrictlyRequired) {
                    throw x;
                }
                fetchError = true;
            }
            // Pass other exceptions through.

            try {
                return getCachedObjectForTxn(txn, id, null, classOfV, updatesListener, !fetchError);
            } catch (NoSuchObjectException x) {
                logger.warning("Object not found in the cache just after fetch (retrying): " + x);
            } catch (VersionNotFoundException x) {
                logger.warning("No self-consistent version found in cache (retrying): " + x);
            }
        }
    }

    @Override
    public <V extends CRDT<V>> TxnLocalCRDT<V> getObjectVersionTxnView(AbstractTxnHandle txn, final CRDTIdentifier id,
            final CausalityClock version, final boolean create, Class<V> classOfV,
            final ObjectUpdatesListener updatesListener) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        assertPendingTransaction(txn);

        try {
            final TxnLocalCRDT<V> view = getCachedObjectForTxn(txn, id, version, classOfV, updatesListener, false);
            cacheStats.addCacheHit(id);
            return view;
        } catch (NoSuchObjectException x) {
            cacheStats.addCacheMissNoObject(id);
            // Ok, let's try to fetch then.
        } catch (VersionNotFoundException x) {
            cacheStats.addCacheMissWrongVersion(id);
            // Ok, let's try to fetch the right version.
        }

        while (true) {
            final CausalityClock globalVersion = version.clone();
            globalVersion.drop(scoutId);
            fetchObjectVersion(txn, id, create, classOfV, globalVersion, true, updatesListener != null);

            try {
                return getCachedObjectForTxn(txn, id, version.clone(), classOfV, updatesListener, true);
            } catch (NoSuchObjectException x) {
                logger.warning("Object not found in the cache just after fetch (retrying): " + x);
            } catch (VersionNotFoundException x) {
                logger.warning("Object not found in appropriate version, probably pruned: " + x);
                throw x;
            }
        }
    }

    /**
     * Returns a view of an object version from the cache, if the object is
     * available in the appropriate version.
     * 
     * @param id
     * @param clock
     *            requested object version or null to specify the most recent
     *            version available
     * @param classOfV
     * @param updatesListener
     * @return object view
     * @throws WrongTypeException
     * @throws NoSuchObjectException
     * @throws VersionNotFoundException
     */
    @SuppressWarnings("unchecked")
    private synchronized <V extends CRDT<V>> TxnLocalCRDT<V> getCachedObjectForTxn(final AbstractTxnHandle txn,
            CRDTIdentifier id, CausalityClock clock, Class<V> classOfV, ObjectUpdatesListener updatesListener,
            boolean justFetched) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException {
        V crdt;
        try {
            crdt = (V) objectsCache.getAndTouch(id);
        } catch (ClassCastException x) {
            throw new WrongTypeException(x.getMessage());
        }
        if (crdt == null) {
            throw new NoSuchObjectException("Object not available in the cache");
        }

        if (clock == null) {
            // Set the requested clock to the latest committed version including
            // prior scout's transactions (the most recent thing we would like
            // to read).
            clock = getCommittedVersion(true);
            clock.merge(lastLocallyCommittedTxnClock);
            if (concurrentOpenTransactions && !txn.isReadOnly()) {
                // Make sure we do not introduce cycles in dependencies. Include
                // only transactions with lower timestamp in the snapshot,
                // because timestamp order induces the commit order.
                clock.drop(scoutId);
                clock.recordAllUntil(txn.getTimestampMapping().getClientTimestamp());
            }

            // Check if such a recent version is available in the cache. If not,
            // take the intersection of the clocks.
            clock.intersect(crdt.getClock());
            // TODO: Discuss. This is a very aggressive caching mode.
        }

        final TxnLocalCRDT<V> crdtView;
        try {
            crdtView = crdt.getTxnLocalCopy(clock, txn);
        } catch (IllegalStateException x) {
            // No appropriate version found in the object from the cache.
            throw new VersionNotFoundException("Object not available in the cache in appropriate version: "
                    + x.getMessage());
        }

        if (updatesListener != null) {
            if (updatesListener.isSubscriptionOnly()) {
                addUpdateSubscriptionNoListener(crdt, !justFetched);
            } else {
                final UpdateSubscriptionWithListener subscription = addUpdateSubscriptionWithListener(txn, crdt,
                        crdtView, updatesListener, !justFetched);
                // Trigger update listener if we already know updates more
                // recent than the returned version.
                handleObjectNewVersionTryNotify(id, subscription, crdt);
            }
        }
        return crdtView;
    }

    private <V extends CRDT<V>> void fetchObjectVersion(final AbstractTxnHandle txn, CRDTIdentifier id, boolean create,
            Class<V> classOfV, final CausalityClock globalVersion, final boolean strictUnprunedVersion,
            final boolean subscribeUpdates) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException {
        // TODO: When deltas are supported, uncomment this code.

        // final V crdt;
        // synchronized (this) {
        // try {
        // crdt = (V) objectsCache.getWithoutTouch(id);
        // } catch (ClassCastException x) {
        // throw new WrongTypeException(x.getMessage());
        // }
        // }
        //
        // if (crdt == null) {
        fetchObjectFromScratch(txn, id, create, classOfV, globalVersion, strictUnprunedVersion, subscribeUpdates);
        // } else {
        // fetchObjectByRefresh(txn, id, create, classOfV, crdt, globalVersion,
        // strictUnprunedVersion,
        // subscribeUpdates);
        // }
    }

    @SuppressWarnings("unchecked")
    private <V extends CRDT<V>> void fetchObjectFromScratch(final AbstractTxnHandle txn, CRDTIdentifier id,
            boolean create, Class<V> classOfV, CausalityClock version, boolean strictUnprunedVersion,
            boolean subscribeUpdates) throws NoSuchObjectException, WrongTypeException, VersionNotFoundException,
            NetworkException {
        final SubscriptionType subscriptionType = subscribeUpdates ? SubscriptionType.UPDATES : SubscriptionType.NONE;
        final FetchObjectVersionRequest fetchRequest = new FetchObjectVersionRequest(scoutId, id, version,
                strictUnprunedVersion, subscriptionType);
        doFetchObjectVersionOrTimeout(txn, fetchRequest, classOfV, create);
    }

    // @SuppressWarnings("unchecked")
    // private <V extends CRDT<V>> void fetchObjectByRefresh(AbstractTxnHandle
    // txn, CRDTIdentifier id, boolean create,
    // Class<V> classOfV, V cachedCrdt, CausalityClock version, boolean
    // strictUnrpunedVersion,
    // boolean subscribeUpdates) throws NoSuchObjectException,
    // WrongTypeException, VersionNotFoundException,
    // NetworkException {
    // final CausalityClock oldCrdtClock;
    // synchronized (this) {
    // oldCrdtClock = cachedCrdt.getClock().clone();
    // oldCrdtClock.drop(clientId);
    // assertIsGlobalClock(oldCrdtClock);
    // }
    //
    // // WISHME: we should replace it with deltas or operations list
    // final SubscriptionType subscriptionType = subscribeUpdates ?
    // SubscriptionType.UPDATES : SubscriptionType.NONE;
    // final FetchObjectDeltaRequest fetchRequest = new
    // FetchObjectDeltaRequest(clientId, id, oldCrdtClock, version,
    // strictUnrpunedVersion, subscriptionType);
    // doFetchObjectVersionOrTimeout(txn, fetchRequest, classOfV, create);
    // }

    private <V extends CRDT<V>> void doFetchObjectVersionOrTimeout(final AbstractTxnHandle txn,
            final FetchObjectVersionRequest fetchRequest, Class<V> classOfV, boolean create) throws NetworkException,
            NoSuchObjectException, WrongTypeException {
        synchronized (this) {
            fetchVersionsInProgress.add(fetchRequest.getVersion());
        }

        try {
            final long firstRequestTimestamp = System.currentTimeMillis();
            FetchObjectVersionReply reply;
            do {
                final long requestDeadline = deadlineMillis - (System.currentTimeMillis() - firstRequestTimestamp);
                if (requestDeadline <= 0) {
                    throw new NetworkException("Deadline exceeded to get appropriate answer from the store;"
                            + "note it may be caused by prior errors");
                }
                reply = retryableTaskExecutor.execute(new CallableWithDeadline<FetchObjectVersionReply>(null,
                        requestDeadline) {

                    public String toString() {
                        return "FetchObjectVersionRequest";
                    }

                    @Override
                    protected FetchObjectVersionReply callOrFailWithNull() {
                        localEndpoint.send(serverEndpoint, fetchRequest, new FetchObjectVersionReplyHandler() {
                            @Override
                            public void onReceive(RpcHandle handle, FetchObjectVersionReply reply) {
                                setResult(reply);
                            }
                        }, Math.min(timeoutMillis, getDeadlineLeft()));
                        return getResult();
                    }
                });
                if (reply == null) {
                    throw new NetworkException("Fetching object version exceeded the deadline");
                }
                if (stopFlag) {
                    throw new NetworkException("Fetching object version was interrupted by scout shutdown.");
                }
            } while (!processFetchObjectReply(txn, fetchRequest, reply, classOfV, create));
        } finally {
            synchronized (this) {
                fetchVersionsInProgress.remove(fetchRequest.getVersion());
            }
        }
    }

    /**
     * @return when the request was successful
     */
    private <V extends CRDT<V>> boolean processFetchObjectReply(final AbstractTxnHandle txn,
            final FetchObjectVersionRequest request, final FetchObjectVersionReply fetchReply, Class<V> classOfV,
            boolean create) throws NoSuchObjectException, WrongTypeException {
        final V crdt;
        switch (fetchReply.getStatus()) {
        case OBJECT_NOT_FOUND:
            if (!create) {
                throw new NoSuchObjectException("object " + request.getUid() + " not found");
            }
            try {
                crdt = classOfV.newInstance();
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }

            crdt.init(request.getUid(), fetchReply.getVersion(), ClockFactory.newClock(), false);
            break;
        case VERSION_NOT_FOUND:
        case OK:
            try {
                crdt = (V) fetchReply.getCrdt();
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }
            crdt.init(request.getUid(), fetchReply.getVersion(), fetchReply.getPruneClock(), true);
            break;
        default:
            throw new IllegalStateException("Unexpected status code" + fetchReply.getStatus());
        }

        synchronized (this) {
            updateCommittedVersions(fetchReply.getEstimatedCommittedVersion(),
                    fetchReply.getEstimatedDisasterDurableCommittedVersion());

            V cacheCRDT;
            try {
                if (txn != null) {
                    cacheCRDT = (V) objectsCache.getAndTouch(request.getUid());
                } else {
                    cacheCRDT = (V) objectsCache.getWithoutTouch(request.getUid());
                }
            } catch (Exception e) {
                throw new WrongTypeException(e.getMessage());
            }

            if (cacheCRDT == null
                    || crdt.getClock().compareTo(cacheCRDT.getClock())
                            .is(CMP_CLOCK.CMP_DOMINATES, CMP_CLOCK.CMP_EQUALS)) {
                // If clock >= cacheCrdt.clock, it 1) does not make sense to
                // merge, 2) received version may have different pruneClock,
                // which could be either helpful or not depending on the case.
                objectsCache.add(crdt);
                cacheCRDT = crdt;
            } else {
                cacheCRDT.merge(crdt);
            }
            // Apply any local updates that may not be present in received
            // version.
            for (final AbstractTxnHandle localTxn : globallyCommittedUnstableTxns) {
                applyLocalObjectUpdates(request.getUid(), cacheCRDT, localTxn);
            }
            for (final AbstractTxnHandle localTxn : locallyCommittedTxnsOrderedQueue) {
                applyLocalObjectUpdates(request.getUid(), cacheCRDT, localTxn);
            }

            Map<String, UpdateSubscriptionWithListener> sessionsSubs = objectSessionsUpdateSubscriptions.get(request
                    .getUid());
            if (request.getSubscriptionType() != SubscriptionType.NONE && sessionsSubs == null) {
                // Add temporary subscription entry without specifying full
                // information on what value has been read.
                addUpdateSubscriptionNoListener(crdt, false);
            }

            // See if anybody is interested in new updates on this object.
            if (sessionsSubs != null) {
                for (final UpdateSubscriptionWithListener subscription : sessionsSubs.values()) {
                    handleObjectNewVersionTryNotify(crdt.getUID(), subscription, cacheCRDT);
                }
            }
        }

        if (fetchReply.getStatus() == FetchStatus.VERSION_NOT_FOUND) {
            logger.warning("requested object version not found in the store, retrying fetch");
            return false;
        }
        return true;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void applyLocalObjectUpdates(final CRDTIdentifier id, CRDT cachedCRDT, final AbstractTxnHandle localTxn) {
        // Try to apply changes in a cached copy of an object.
        if (cachedCRDT == null) {
            logger.warning("object evicted from the local cache, cannot apply local transaction changes");
            return;
        }

        final CRDTObjectUpdatesGroup objectUpdates = localTxn.getObjectUpdates(id);
        if (objectUpdates != null) {
            // IGNORE dependencies checking, for RR transaction
            // dependencies are overestimated.
            // TODO: during failover, it may be unsafe to IGNORE.
            cachedCRDT.execute(objectUpdates, CRDTOperationDependencyPolicy.IGNORE);
        }
    }

    private void fetchSubscribedNotifications() {
        final AtomicReference<FastRecentUpdatesReply> replyRef = new AtomicReference<FastRecentUpdatesReply>();
        localEndpoint.send(serverEndpoint,
                new FastRecentUpdatesRequest(scoutId, Math.max(0, notificationTimeoutMillis - timeoutMillis)),
                new FastRecentUpdatesReplyHandler() {
                    @Override
                    public void onReceive(RpcHandle conn, FastRecentUpdatesReply reply) {
                        replyRef.set(reply);
                    }
                }, notificationTimeoutMillis);
        final FastRecentUpdatesReply notifications = replyRef.get();
        if (notifications == null) {
            logger.warning("server did not reply with recent update notifications");
            return;
        }
        if (logger.isLoggable(Level.INFO)) {
            logger.info("notifications received for " + notifications.getSubscriptions().size() + " objects" + ";vrs="
                    + notifications.getEstimatedCommittedVersion() + ";stable="
                    + notifications.getEstimatedDisasterDurableCommittedVersion());
            if (notifications.getSubscriptions().size() > 0) {
                ObjectSubscriptionInfo sub = notifications.getSubscriptions().get(0);
                logger.info("notifications received in " + scoutId + " for " + sub.getId() + "; old clk: "
                        + sub.getOldClock() + "; new clk " + sub.getNewClock());
            }
        }

        updateCommittedVersions(notifications.getEstimatedCommittedVersion(),
                notifications.getEstimatedDisasterDurableCommittedVersion());
        if (notifications.getStatus() == SubscriptionStatus.ACTIVE) {
            // Process notifications.
            for (final ObjectSubscriptionInfo subscriptionInfo : notifications.getSubscriptions()) {
                if (subscriptionInfo.isDirty() && subscriptionInfo.getUpdates().isEmpty()) {
                    logger.warning("unexpected server notification information without update");
                } else {
                    applyObjectUpdates(subscriptionInfo.getId(), subscriptionInfo.getOldClock(),
                            subscriptionInfo.getUpdates(), subscriptionInfo.getNewClock(),
                            subscriptionInfo.getPruneClock());
                }
            }
        } else {
            // Renew lost subscriptions.
            synchronized (this) {
                for (final CRDTIdentifier id : objectSessionsUpdateSubscriptions.keySet()) {
                    asyncFetchAndSubscribeObjectUpdates(id);
                }
            }
        }
    }

    /**
     * @return true if subscription should be continued for this object
     */
    private synchronized void applyObjectUpdates(final CRDTIdentifier id, final CausalityClock dependencyClock,
            final List<CRDTObjectUpdatesGroup<?>> ops, final CausalityClock outputClock, final CausalityClock pruneClock) {
        if (stopFlag) {
            logger.info("Update received after scout has been stopped -> ignoring");
            return;
        }

        final Map<String, UpdateSubscriptionWithListener> sessionsSubs = objectSessionsUpdateSubscriptions.get(id);
        if (sessionsSubs == null) {
            removeUpdateSubscriptionAsyncUnsubscribe(id);
        }

        final CRDT crdt = objectsCache.getWithoutTouch(id);
        if (crdt == null) {
            // Ooops, we evicted the object from the cache.
            logger.info("cannot apply received updates on object " + id + " as it has been evicted from the cache");
            if (sessionsSubs != null) {
                if (!sessionsSubs.isEmpty()) {
                    if (!ops.isEmpty()) {
                        // There is still listener waiting, make some efforts to
                        // fire the notification.
                        asyncFetchAndSubscribeObjectUpdates(id);
                    }
                } else {
                    // Stop subscription for object evicted from the cache.
                    removeUpdateSubscriptionAsyncUnsubscribe(id);
                }
            }
            return;
        }

        if (crdt.getClock().compareTo(dependencyClock).is(CMP_CLOCK.CMP_ISDOMINATED, CMP_CLOCK.CMP_CONCURRENT)) {
            // Ooops, we missed some update or messages were ordered.
            logger.info("cannot apply received updates on object " + id + " due to unsatisfied dependencies");
            if (sessionsSubs != null && !ops.isEmpty()) {
                asyncFetchAndSubscribeObjectUpdates(id);
            }
            return;
        }

        if (logger.isLoggable(Level.INFO)) {
            // TODO: printf usage wouldn't hurt :-)
            logger.info("applying received updates on object " + id + ";num.ops=" + ops.size() + ";tx="
                    + (ops.size() == 0 ? "-" : ops.get(0).getTimestampMapping().getSelectedSystemTimestamp())
                    + ";clttx=" + (ops.size() == 0 ? "-" : ops.get(0).getTimestamps().get(0)) + ";vv=" + outputClock
                    + ";dep=" + dependencyClock);
        }

        for (final CRDTObjectUpdatesGroup<?> op : ops) {
            final boolean newUpdate = crdt.execute(op, CRDTOperationDependencyPolicy.RECORD_BLINDLY);
            final String updatesScoutId = op.getClientTimestamp().getIdentifier();
            if (!updatesScoutId.equals(scoutId)) {
                crdt.discardScoutClock(updatesScoutId);
            }
            if (!newUpdate) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("update " + op.getClientTimestamp() + " was already included in the state of object "
                            + id);
                }
                // Already applied update.
                continue;
            }
            if (sessionsSubs != null) {
                for (final UpdateSubscriptionWithListener subscription : sessionsSubs.values()) {
                    handleObjectUpdatesTryNotify(id, subscription, op.getTimestampMapping());
                }
            }
        }
        crdt.getClock().merge(outputClock);
        crdt.prune(pruneClock, true);
    }

    private synchronized void handleObjectUpdatesTryNotify(CRDTIdentifier id,
            UpdateSubscriptionWithListener subscription, TimestampMapping... timestampMappings) {
        if (stopFlag) {
            logger.info("Update received after scout has been stopped -> ignoring");
            return;
        }

        Map<TimestampMapping, CRDTIdentifier> uncommittedUpdates = new HashMap<TimestampMapping, CRDTIdentifier>();
        for (final TimestampMapping tm : timestampMappings) {
            if (!tm.anyTimestampIncluded(subscription.readVersion)) {
                if (tm.anyTimestampIncluded(getCommittedVersion(false))) {
                    notificationsCallbacksExecutor.execute(subscription.generateNotificationAndDiscard(this, id));
                    return;
                }
                uncommittedUpdates.put(tm, id);
            }
        }
        // There was no committed timestamp we could notify about, so put
        // them the queue of updates to notify when they are committed.
        for (final Entry<TimestampMapping, CRDTIdentifier> entry : uncommittedUpdates.entrySet()) {
            Set<CRDTIdentifier> ids = uncommittedUpdatesObjectsToNotify.get(entry.getKey());
            if (ids == null) {
                ids = new HashSet<CRDTIdentifier>();
                uncommittedUpdatesObjectsToNotify.put(entry.getKey().copy(), ids);
            } else {
                // FIXME: merge timestamp mappings for entry.getKey().
                // TRICKY! Should we also apply these mappings to objects, it
                // must be consistent!
            }
            ids.add(entry.getValue());
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Update on object " + id + " visible, but not committed, delaying notification");
            }
        }
    }

    private synchronized <V extends CRDT<V>> void handleObjectNewVersionTryNotify(CRDTIdentifier id,
            final UpdateSubscriptionWithListener subscription, final V newCrdtVersion) {
        if (stopFlag) {
            logger.info("Update received after scout has been stopped -> ignoring");
            return;
        }

        final Set<TimestampMapping> recentUpdates;
        try {
            recentUpdates = newCrdtVersion.getUpdatesTimestampMappingsSince(subscription.readVersion);
        } catch (IllegalArgumentException x) {
            // Object has been pruned since then, approximate by comparing old
            // and new txn views. This is a very bizzare case.
            logger.warning("Object has been pruned since notification was set up, needs to investigate the observable view");
            final TxnLocalCRDT<V> newView = newCrdtVersion
                    .getTxnLocalCopy(getCommittedVersion(false), subscription.txn);
            if (!newView.getValue().equals(subscription.crdtView.getValue())) {
                notificationsCallbacksExecutor.execute(subscription.generateNotificationAndDiscard(this, id));
            }
            return;
        }
        handleObjectUpdatesTryNotify(id, subscription, recentUpdates.toArray(new TimestampMapping[0]));
    }

    private synchronized void addUpdateSubscriptionNoListener(final CRDT<?> crdt, boolean needsFetch) {
        if (!objectSessionsUpdateSubscriptions.containsKey(crdt.getUID())) {
            objectSessionsUpdateSubscriptions.put(crdt.getUID(), new HashMap<String, UpdateSubscriptionWithListener>());
            if (needsFetch && crdt.isRegisteredInStore()) {
                asyncFetchAndSubscribeObjectUpdates(crdt.getUID());
            }
            // else: newly created object, wait until untilcommitTxnGlobally()
            // with subscription.
        }
    }

    private synchronized UpdateSubscriptionWithListener addUpdateSubscriptionWithListener(final AbstractTxnHandle txn,
            final CRDT<?> crdt, final TxnLocalCRDT<?> localView, ObjectUpdatesListener listener, boolean needsFetch) {
        if (listener.isSubscriptionOnly()) {
            throw new IllegalArgumentException("Dummy listener subscribing udpates like a real listener for object "
                    + crdt.getUID());
        }

        Map<String, UpdateSubscriptionWithListener> sessionsSubs = objectSessionsUpdateSubscriptions.get(crdt.getUID());
        if (sessionsSubs == null) {
            addUpdateSubscriptionNoListener(crdt, needsFetch);
            sessionsSubs = objectSessionsUpdateSubscriptions.get(crdt.getUID());
        }

        final UpdateSubscriptionWithListener updateSubscription = new UpdateSubscriptionWithListener(txn, localView,
                listener);
        // Overwriting old session entry and even subscribing again is fine, the
        // interface specifies clearly that the latest get() matters.
        sessionsSubs.put(txn.getSessionId(), updateSubscription);
        return updateSubscription;
    }

    private synchronized void removeUpdateSubscriptionWithListener(CRDTIdentifier id, String sessionId,
            UpdateSubscriptionWithListener listener) {
        final Map<String, UpdateSubscriptionWithListener> sessionsSubs = objectSessionsUpdateSubscriptions.get(id);
        if (sessionsSubs != null && sessionsSubs.get(sessionId) == listener) {
            sessionsSubs.remove(sessionId);
        }
    }

    private void asyncFetchAndSubscribeObjectUpdates(final CRDTIdentifier id) {
        if (stopFlag) {
            logger.info("Update received after scout has been stopped -> ignoring");
            return;
        }

        notificationsSubscriberExecutor.execute(new Runnable() {
            @Override
            public void run() {
                final CausalityClock version;
                synchronized (SwiftImpl.this) {
                    if (!objectSessionsUpdateSubscriptions.containsKey(id)) {
                        return;
                    }
                    version = getCommittedVersion(true);
                    version.merge(lastLocallyCommittedTxnClock);
                    version.drop(scoutId);
                }
                try {
                    fetchObjectVersion(null, id, false, BaseCRDT.class, version, false, true);
                } catch (SwiftException x) {
                    logger.warning("could not fetch the latest version of an object for notifications purposes: "
                            + x.getMessage());
                }
            }
        });
    }

    private synchronized void removeUpdateSubscriptionAsyncUnsubscribe(final CRDTIdentifier id) {
        objectSessionsUpdateSubscriptions.remove(id);
        notificationsSubscriberExecutor.execute(new Runnable() {
            @Override
            public void run() {
                if (objectSessionsUpdateSubscriptions.containsKey(id)) {
                    return;
                }
                if (localEndpoint.send(serverEndpoint, new UnsubscribeUpdatesRequest(scoutId, id)).failed()) {
                    logger.info("failed to unsuscribe object updates");
                }
            }
        });
    }

    @Override
    public synchronized void discardTxn(AbstractTxnHandle txn) {
        assertPendingTransaction(txn);
        removePendingTxn(txn);
        logger.info("local transaction " + txn.getTimestampMapping() + " rolled back");
        if (requiresGlobalCommit(txn)) {
            // Need to create and commit a dummy transaction, we cannot
            // returnLastTimestamp :-(
            final RepeatableReadsTxnHandle dummyTxn = new RepeatableReadsTxnHandle(this, txn.getSessionId(),
                    durableLog, CachePolicy.CACHED, txn.getTimestampMapping());
            dummyTxn.markLocallyCommitted();
            commitTxn(dummyTxn);
        } else {
            tryReuseTxnTimestamp(txn);
        }
    }

    private boolean requiresGlobalCommit(AbstractTxnHandle txn) {
        if (txn.isReadOnly()) {
            return false;
        }
        if (!concurrentOpenTransactions) {
            if (txn.getStatus() == TxnStatus.CANCELLED || txn.getAllUpdates().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized void commitTxn(AbstractTxnHandle txn) {
        assertPendingTransaction(txn);
        assertRunning();

        txn.markLocallyCommitted();
        if (logger.isLoggable(Level.INFO)) {
            logger.info("transaction " + txn.getTimestampMapping() + " commited locally");
        }
        if (requiresGlobalCommit(txn)) {
            for (final CRDTObjectUpdatesGroup opsGroup : txn.getAllUpdates()) {
                final CRDTIdentifier id = opsGroup.getTargetUID();
                applyLocalObjectUpdates(id, objectsCache.getWithoutTouch(id), txn);
                // Look if there is any other session to notify.
                final Map<String, UpdateSubscriptionWithListener> sessionsSubs = objectSessionsUpdateSubscriptions
                        .get(id);
                if (sessionsSubs != null) {
                    for (final UpdateSubscriptionWithListener subscription : sessionsSubs.values()) {
                        handleObjectUpdatesTryNotify(id, subscription, opsGroup.getTimestampMapping());
                    }
                }
            }
            lastLocallyCommittedTxnClock.record(txn.getTimestampMapping().getClientTimestamp());
            lastLocallyCommittedTxnClock.merge(txn.getUpdatesDependencyClock());
            objectsCache.recordOnAll(txn.getTimestampMapping());

            // Transaction is queued up for global commit.
            // THIS MAY BLOCK in wait() if the queue is full!
            addLocallyCommittedTransactionBlocking(txn);
        } else {
            tryReuseTxnTimestamp(txn);
            txn.markGloballyCommitted(null);
            if (logger.isLoggable(Level.INFO)) {
                logger.info("read-only transaction " + txn.getTimestampMapping() + " will not commit globally");
            }
        }
        removePendingTxn(txn);
        objectsCache.evictOutdated();
    }

    private void tryReuseTxnTimestamp(AbstractTxnHandle txn) {
        if (!txn.isReadOnly()) {
            returnLastTimestamp();
        }
    }

    /**
     * Stubborn commit procedure. Repeats until it succeeds.
     * 
     * @param txn
     *            locally committed transaction
     */
    private void commitTxnGlobally(final List<AbstractTxnHandle> transactionsToCommit) {
        final List<CommitUpdatesRequest> requests = new LinkedList<CommitUpdatesRequest>();
        // Preprocess transactions before sending them.
        for (final AbstractTxnHandle txn : transactionsToCommit) {
            txn.assertStatus(TxnStatus.COMMITTED_LOCAL);

            txn.getUpdatesDependencyClock().drop(scoutId);
            // Use optimizedDependencyClock when sending out the updates - it
            // may impose more restrictions, but contains less holes.
            final CausalityClock optimizedDependencyClock = getCommittedVersion(true);
            optimizedDependencyClock.merge(txn.getUpdatesDependencyClock());
            optimizedDependencyClock.drop(scoutId);
            final LinkedList<CRDTObjectUpdatesGroup<?>> operationsGroups = new LinkedList<CRDTObjectUpdatesGroup<?>>();
            for (final CRDTObjectUpdatesGroup<?> group : txn.getAllUpdates()) {
                operationsGroups.add(group.withWithDependencyClock(optimizedDependencyClock));
            }
            requests.add(new CommitUpdatesRequest(scoutId, txn.getTimestampMapping().getClientTimestamp(), txn
                    .getUpdatesDependencyClock(), operationsGroups));
        }

        // Commit at server.
        final BatchCommitUpdatesReply batchReply = retryableTaskExecutor
                .execute(new CallableWithDeadline<BatchCommitUpdatesReply>(null) {
                    public String toString() {
                        return "BatchCommitUpdatesRequest";
                    }

                    @Override
                    protected BatchCommitUpdatesReply callOrFailWithNull() {
                        localEndpoint.send(serverEndpoint, new BatchCommitUpdatesRequest(scoutId, requests),
                                new BatchCommitUpdatesReplyHandler() {
                                    @Override
                                    public void onReceive(RpcHandle conn, BatchCommitUpdatesReply reply) {
                                        setResult(reply);
                                    }
                                }, timeoutMillis);
                        return super.getResult();
                    }
                });

        if (batchReply.getReplies().size() != requests.size()) {
            throw new IllegalStateException("Fatal error: server returned " + batchReply.getReplies().size() + " for "
                    + requests.size() + " commit requests!");
        }

        synchronized (this) {
            for (int i = 0; i < batchReply.getReplies().size(); i++) {
                final CommitUpdatesReply reply = batchReply.getReplies().get(i);
                final AbstractTxnHandle txn = transactionsToCommit.get(i);
                txn.updateUpdatesDependencyClock(lastGloballyCommittedTxnClock);
                switch (reply.getStatus()) {
                case COMMITTED_WITH_KNOWN_TIMESTAMPS:
                    for (final Timestamp ts : reply.getCommitTimestamps()) {
                        txn.markGloballyCommitted(ts);
                        lastGloballyCommittedTxnClock.record(ts);
                        committedVersion.record(ts);
                        // TODO: call updateCommittedVersion?
                    }
                    objectsCache.recordOnAll(txn.getTimestampMapping());
                    break;
                case COMMITTED_WITH_KNOWN_CLOCK_RANGE:
                    lastGloballyCommittedTxnClock.merge(reply.getImpreciseCommitClock());
                    txn.markGloballyCommitted(null);
                    // TODO: call updateCommittedVersion?
                    break;
                case INVALID_OPERATION:
                    throw new IllegalStateException("DC replied to commit request with INVALID_OPERATION");
                    // break;
                default:
                    throw new UnsupportedOperationException("unknown commit status: " + reply.getStatus());
                }

                lastGloballyCommittedTxnClock.merge(txn.getUpdatesDependencyClock());
                lastLocallyCommittedTxnClock.merge(lastGloballyCommittedTxnClock);
                removeLocallyNowGloballyCommitedTxn(txn);
                globallyCommittedUnstableTxns.addLast(txn);

                if (logger.isLoggable(Level.INFO)) {
                    logger.info("transaction " + txn.getTimestampMapping() + " commited globally");
                }

                // Subscribe updates for newly created objects if they were
                // requested. It can be done only at this stage once the objects
                // are in the store.
                for (final CRDTObjectUpdatesGroup opsGroup : txn.getAllUpdates()) {
                    final boolean subscriptionsExist = objectSessionsUpdateSubscriptions.containsKey(opsGroup
                            .getTargetUID());
                    if (subscriptionsExist && opsGroup.hasCreationState()) {
                        asyncFetchAndSubscribeObjectUpdates(opsGroup.getTargetUID());
                    }
                }
            }
        }
    }

    private synchronized void addLocallyCommittedTransactionBlocking(AbstractTxnHandle txn) {
        // Insert only if the queue size allows, or if tnx blocks other
        // transactions.
        while (locallyCommittedTxnsOrderedQueue.size() >= maxAsyncTransactionsQueued
                && locallyCommittedTxnsOrderedQueue.first().compareTo(txn) < 0) {
            logger.warning("Asynchronous commit queue is full - blocking the transaction commit");
            try {
                this.wait();
            } catch (InterruptedException e) {
                if (stopFlag && !stopGracefully) {
                    throw new IllegalStateException("Scout stopped in non-graceful manner, transaction not commited");
                }
            }
        }
        locallyCommittedTxnsOrderedQueue.add(txn);
        // Notify committer thread.
        this.notifyAll();
    }

    private synchronized void removeLocallyNowGloballyCommitedTxn(final AbstractTxnHandle txn) {
        locallyCommittedTxnsOrderedQueue.remove(txn);
        this.notifyAll();
    }

    /**
     * @return a batch of transactions ready to commit (within
     *         maxCommitBatchSize limit)
     */
    private synchronized List<AbstractTxnHandle> consumeLocallyCommitedTxnsQueue() {
        List<AbstractTxnHandle> result = new LinkedList<AbstractTxnHandle>();
        do {
            final Iterator<AbstractTxnHandle> queueIter = locallyCommittedTxnsOrderedQueue.iterator();
            for (int i = 0; i < maxCommitBatchSize && queueIter.hasNext(); i++) {
                final AbstractTxnHandle candidateTxn = queueIter.next();
                boolean validCandidate = true;
                if (concurrentOpenTransactions) {
                    // Check whether transactions with lower timestamps already
                    // committed. TODO: this is a quick HACK, do it better.
                    final long candidateCounter = locallyCommittedTxnsOrderedQueue.first().getTimestampMapping()
                            .getClientTimestamp().getCounter();
                    for (final AbstractTxnHandle txn : pendingTxns) {
                        if (!txn.isReadOnly()
                                && txn.getTimestampMapping().getClientTimestamp().getCounter() < candidateCounter) {
                            validCandidate = false;
                            break;
                        }
                    }
                }

                if (validCandidate) {
                    result.add(candidateTxn);
                } else {
                    break;
                }
            }

            if (result.size() > maxCommitBatchSize) {
                throw new IllegalStateException("Internal error, transaction batch size computed wrongly");
            }
            if (result.isEmpty() && !stopFlag) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                }
            } else {
                return result;
            }
        } while (true);
    }

    private synchronized void addPendingTxn(final AbstractTxnHandle txn) {
        pendingTxns.add(txn);
    }

    private synchronized void removePendingTxn(final AbstractTxnHandle txn) {
        pendingTxns.remove(txn);
    }

    private synchronized void assertPendingTransaction(final AbstractTxnHandle expectedTxn) {
        if (!pendingTxns.contains(expectedTxn)) {
            throw new IllegalStateException(
                    "Corrupted state: unexpected transaction is bothering me, not the pending one");
        }
    }

    private void assertRunning() {
        if (stopFlag) {
            throw new IllegalStateException("scout is stopped");
        }
    }

    private void assertIsGlobalClock(CausalityClock version) {
        if (version.hasEventFrom(scoutId)) {
            throw new IllegalArgumentException("transaction requested visibility of local transaction");
        }
    }

    /**
     * Thread continuously committing locally committed transactions. The thread
     * takes the oldest locally committed transaction one by one, tries to
     * commit it to the store and update relevant clock information.
     */
    private class CommitterThread extends Thread {

        public CommitterThread() {
            super("SwiftTransactionCommitterThread");
        }

        @Override
        public void run() {
            while (true) {
                List<AbstractTxnHandle> transactionsToCommit = consumeLocallyCommitedTxnsQueue();
                if (stopFlag && (transactionsToCommit.isEmpty() || !stopGracefully)) {
                    if (!transactionsToCommit.isEmpty()) {
                        logger.warning("Scout ungraceful stop, some transactions may not globally committed");
                    }
                    return;
                }
                commitTxnGlobally(transactionsToCommit);
            }
        }
    }

    private class NotoficationsProcessorThread extends Thread {
        public NotoficationsProcessorThread() {
            super("SwiftNotificationsProcessorThread");
            setDaemon(true);
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
     * Scout representation of updates subscription with listener for a session.
     * The listener is awaiting for notification on update that occurred after
     * the readVersion.
     * 
     * @author mzawirski
     */
    private static class UpdateSubscriptionWithListener {
        private final AbstractTxnHandle txn;
        private final ObjectUpdatesListener listener;
        private final TxnLocalCRDT<?> crdtView;
        private final CausalityClock readVersion;
        private final AtomicBoolean fired;

        public UpdateSubscriptionWithListener(AbstractTxnHandle txn, TxnLocalCRDT<?> crdtView,
                final ObjectUpdatesListener listener) {
            this.txn = txn;
            this.crdtView = crdtView;
            this.listener = listener;
            this.readVersion = crdtView.getClock().clone();
            this.fired = new AtomicBoolean();
        }

        public Runnable generateNotificationAndDiscard(final SwiftImpl scout, final CRDTIdentifier id) {
            return new Runnable() {
                @Override
                public void run() {
                    if (fired.getAndSet(true)) {
                        return;
                    }
                    logger.info("Notifying on update on object " + id);
                    listener.onObjectUpdate(txn, id, crdtView);
                    // Mummy (well, daddy) tells you: clean up after yourself.
                    scout.removeUpdateSubscriptionWithListener(id, txn.getSessionId(),
                            UpdateSubscriptionWithListener.this);
                }
            };
        }
    }
}
