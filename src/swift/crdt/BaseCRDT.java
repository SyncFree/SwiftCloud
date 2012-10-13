package swift.crdt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperationDependencyPolicy;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import sys.net.impl.KryoLib;

/**
 * Base class for CRDT implementations for the interface {@CRDT}.
 * 
 * This class provides and manages the generic information needed for
 * versioning, pruning, and caching behavior of CRDT objects in the context of
 * {@Swift} and {@TxnHandle}s.
 * 
 * @author annettebieniusa, mzawirsk
 * 
 * @param <V>
 *            type implementing the CRDT interface
 */
// TODO: A fundamental refactor to discuss: it seems that the complexity of
// having a materialized versioned CRDT is not worth it. Consider representing
// the pruned state as a normal CRDT, and the versioned part as a log (DAG) of
// updates, applied on demand. It would allow us to extract a huge piece of a
// common code (primarly for merge and timestamps/clock management) that is
// currently type-specific and error-prone.
public abstract class BaseCRDT<V extends BaseCRDT<V>> implements CRDT<V> {
    private static final long serialVersionUID = 1L;
    // clock with the current local state of this CRDT, comprises all timestamps
    // of updates and merges performed on this replica
    private transient CausalityClock updatesClock;
    // clock with the prune point
    private transient CausalityClock pruneClock;
    // id under which the CRDT is globally known and uniquely identified
    protected transient CRDTIdentifier id;
    // registration status
    protected transient boolean registeredInStore;
    // mapping from active client-assigned Timestamp to TripleTimestamp
    protected Map<Timestamp, List<TripleTimestamp>> clientTimestampsInUse;

    public BaseCRDT() {
        clientTimestampsInUse = new HashMap<Timestamp, List<TripleTimestamp>>();
    }

    @Override
    public void init(CRDTIdentifier id, CausalityClock clock, CausalityClock pruneClock, boolean registeredInStore) {
        this.id = id;
        this.updatesClock = clock;
        this.pruneClock = pruneClock;
        this.registeredInStore = registeredInStore;
    }

    @Override
    public CausalityClock getClock() {
        return updatesClock;
    }

    @Override
    public CausalityClock getPruneClock() {
        return pruneClock;
    }

    @Override
    public void prune(CausalityClock pruningPoint, boolean checkVersionClock) {
        assertGreaterEqualsPruneClock(pruningPoint);
        if (checkVersionClock
                && updatesClock.compareTo(pruningPoint).is(CMP_CLOCK.CMP_CONCURRENT, CMP_CLOCK.CMP_ISDOMINATED)) {
            throw new IllegalStateException("Cannot prune concurrently or later than updates clock of this version");
        }

        updatesClock.merge(pruningPoint);
        pruneClock.merge(pruningPoint);
        pruneImpl(pruningPoint);
    }

    /**
     * Prunes the object payload from versioning information (e.g. tombstone) of
     * updates that have been performed prior to the pruning point.
     * 
     * @param pruningPoint
     *            clock up to which data clean-up is performed; without
     *            exceptions
     */
    protected abstract void pruneImpl(CausalityClock pruningPoint);

    @Override
    public void augmentWithScoutClock(final Timestamp latestAppliedScoutTimestamp) {
        updatesClock.recordAllUntil(latestAppliedScoutTimestamp);
    }

    @Override
    public void discardScoutClock(final String scoutId) {
        updatesClock.drop(scoutId);
    }

    /**
     * Augments update clock of this object with the vector clock of the server,
     * as the missing transactions guaranteedly have not touched the object
     * 
     * @param currentDCClock
     *            current DC clock
     */
    public void augmentWithDCClock(final CausalityClock currentDCClock) {
        updatesClock.merge(currentDCClock);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void merge(CRDT<V> otherObject) {
        mergePayload((V) otherObject);
        mergeTimestampMappings((V) otherObject);
        getClock().merge(otherObject.getClock());
        pruneClock.merge(otherObject.getPruneClock());
        prune(pruneClock.clone(), false);
        registeredInStore |= otherObject.isRegisteredInStore();
    }

    protected Iterator<TimestampMapping> iteratorTimestampMappings() {
        final Iterator<List<TripleTimestamp>> iter = clientTimestampsInUse.values().iterator();
        return new Iterator<TimestampMapping>() {
            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public TimestampMapping next() {
                // Get any timestamp, they should all use the same mappings.
                return iter.next().get(0).getMapping();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    protected void mergeTimestampMappings(V otherObject) {
        final Iterator<TimestampMapping> iter = otherObject.iteratorTimestampMappings();
        while (iter.hasNext()) {
            updateTimestampUsageMapping(iter.next());
        }
    }

    /**
     * Merges the payload of the object with the payload of another object
     * having the same CRDT type. Note that {@link #pruneImpl(CausalityClock)}
     * is always called immediatelly afterwards, clock and pruneCLock are also
     * updated, but the implementation may already perform some pruning
     * necessary during merge.
     * <p>
     * Newly used timestamp mapping should be registered via
     * {@link #registerTimestampUsage(TripleTimestamp)} during merge and
     * released timestamp mappings should be released via
     * {@link #unregisterTimestampUsage(TripleTimestamp)}; the remaining
     * mappings will be merged by this base class after this call.
     * 
     * @param otherObject
     *            object to merge with
     */
    protected abstract void mergePayload(V otherObject);

    protected void registerTimestampUsage(final TripleTimestamp ts) {
        List<TripleTimestamp> timestampsForClient = clientTimestampsInUse.get(ts.getClientTimestamp());
        if (timestampsForClient == null) {
            timestampsForClient = new LinkedList<TripleTimestamp>();
            clientTimestampsInUse.put(ts.getClientTimestamp(), timestampsForClient);
        }
        boolean updated = false;
        for (final TripleTimestamp existingTs : timestampsForClient) {
            if (!updated) {
                // Update added mappings - unify it with existing
                // timestamps.
                ts.addSystemTimestamps(existingTs.getMapping());
                updated = true;
            }
            // Update existing timestamp mappings - unify them with the added
            // one.
            existingTs.getMapping().addSystemTimestamps(ts.getMapping());
        }
        timestampsForClient.add(ts);
    }

    protected void updateTimestampUsageMapping(final TimestampMapping mapping) {
        List<TripleTimestamp> timestampsForClient = clientTimestampsInUse.get(mapping.getClientTimestamp());
        if (timestampsForClient == null) {
            return;
        }
        for (final TripleTimestamp existingTs : timestampsForClient) {
            existingTs.getMapping().addSystemTimestamps(mapping);
        }
    }

    // FIXME: ideally, clientTimestampsInUse should be a weak reference map, so
    // we do not need to unregister timestamps explicitly, but how it plays with
    // Kryo serialization is unknown.
    protected void unregisterTimestampUsage(final TripleTimestamp ts) {
        List<TripleTimestamp> timestampsForClient = clientTimestampsInUse.get(ts.getClientTimestamp());
        if (timestampsForClient == null) {
            throw new IllegalStateException("Timestamp to unregister cannot be found");
        }
        // Remove precisely one instance.
        if (!timestampsForClient.remove(ts)) {
            throw new IllegalStateException("Could not find timestamp for usage unregister");
        }
        if (timestampsForClient.isEmpty()) {
            clientTimestampsInUse.remove(ts.getClientTimestamp());
        }
    }

    @Override
    public boolean execute(CRDTObjectUpdatesGroup<V> ops, final CRDTOperationDependencyPolicy dependenciesPolicy) {
        final CausalityClock dependencyClock = ops.getDependency();
        if (dependenciesPolicy == CRDTOperationDependencyPolicy.CHECK) {
            final CMP_CLOCK dependencyCmp = updatesClock.compareTo(dependencyClock);
            if (dependencyCmp == CMP_CLOCK.CMP_ISDOMINATED || dependencyCmp == CMP_CLOCK.CMP_CONCURRENT) {
                throw new IllegalStateException("Object does not meet operation's dependencies");
            }
        } else if (dependenciesPolicy == CRDTOperationDependencyPolicy.RECORD_BLINDLY) {
            updatesClock.merge(dependencyClock);
        } // else: dependenciesPolicy ==IGNORE

        if (ops.hasCreationState()) {
            registeredInStore = true;
        }

        boolean newOperation = true;
        for (final Timestamp timestamp : ops.getTimestamps()) {
            newOperation &= updatesClock.record(timestamp);
        }
        if (newOperation) {
            // Apply operations using a separate copy of mappings to ensure
            // isolation.
            final TimestampMapping mappingsBackup = ops.getTimestampMapping();
            final TimestampMapping mappingsCopy = mappingsBackup.copy();
            for (final CRDTUpdate<V> op : ops.getOperations()) {
                op.setTimestampMapping(mappingsCopy);
                execute(op);
                op.setTimestampMapping(mappingsBackup);
            }
        } else {
            updateTimestampUsageMapping(ops.getTimestampMapping());
        }

        return newOperation;
    }

    /**
     * Executes an update operation on the object.
     * 
     * @param op
     *            operation which is executed
     */
    protected abstract void execute(CRDTUpdate<V> op);

    @Override
    public CRDTIdentifier getUID() {
        return this.id;
    }

    @Override
    public boolean isRegisteredInStore() {
        return registeredInStore;
    }

    @Override
    public TxnLocalCRDT<V> getTxnLocalCopy(CausalityClock versionClock, TxnHandle txn) {
        assertGreaterEqualsPruneClock(versionClock);
        assertLessEqualsClock(versionClock);
        return getTxnLocalCopyImpl(versionClock, txn);
    }

    /**
     * Creates a transaction local view of the object in the version that
     * corresponds to the versionClock.
     * 
     * @param versionClock
     *            clock determining the version of the payload
     * @param txn
     *            transaction which requested the transaction-local view
     * @return copy of the transaction-local view of the object from the
     *         snapshot at versionClock
     */
    protected abstract TxnLocalCRDT<V> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn);

    protected void assertLessEqualsClock(CausalityClock clock) {
        if (getClock() == null) {
            return;
        }
        final CMP_CLOCK clockCmp = getClock().compareTo(clock);
        if (clockCmp == CMP_CLOCK.CMP_CONCURRENT || clockCmp == CMP_CLOCK.CMP_ISDOMINATED) {
            throw new IllegalStateException("provided clock is not less or equal to the clock");
        }
    }

    protected void assertGreaterEqualsPruneClock(CausalityClock clock) {
        if (getPruneClock() == null) {
            return;
        }
        final CMP_CLOCK clockCmp = getPruneClock().compareTo(clock);
        if (clockCmp == CMP_CLOCK.CMP_CONCURRENT || clockCmp == CMP_CLOCK.CMP_DOMINATES) {
            throw new IllegalStateException("provided clock is not greater or equal to the prune clock");
        }
    }

    protected void assertPruneClockWithoutExpceptions(CausalityClock clock) {
        if (clock.hasExceptions()) {
            throw new IllegalArgumentException("provided clock has exceptions and cannot be used as prune clock");
        }
    }

    @Override
    public Set<TimestampMapping> getUpdatesTimestampMappingsSince(CausalityClock clock) {
        if (clock.compareTo(pruneClock).is(CMP_CLOCK.CMP_CONCURRENT, CMP_CLOCK.CMP_ISDOMINATED)) {
            throw new IllegalArgumentException();
        }

        final Set<TimestampMapping> result = new HashSet<TimestampMapping>();
        final Iterator<TimestampMapping> iter = iteratorTimestampMappings();
        while (iter.hasNext()) {
            final TimestampMapping mapping = iter.next();
            if (!mapping.anyTimestampIncluded(clock)) {
                result.add(mapping.copy());
            }
        }
        return result;
    }

    @Override
    // TODO Check that all class which override this method either create deep
    // copies or only share immutable information!
    public V copy() {
        final V copy = (V) KryoLib.copy(this);
        // initialize the object with copies of the transient base information
        // which is ignored by Kryo
        copyBase(copy);
        return copy;
    }

    /**
     * Initializes the object with a copy of the base information as needed by
     * transactions and the Swift system.
     * <p>
     * This function should be called as part of the deep copying process to
     * correctly initialize the copy.
     * 
     * @param object
     *            object that gets initialized with the base information of this
     *            object
     */
    protected void copyBase(V object) {
        object.init(id, updatesClock == null ? null : updatesClock.clone(),
                pruneClock == null ? null : pruneClock.clone(), registeredInStore);
        // FIXME: unify copy() implementations and serialization - should this
        // clientTimestampsInUse be here or not (it is not transient)?
        object.clientTimestampsInUse = KryoLib.copy(clientTimestampsInUse);
    }
}
