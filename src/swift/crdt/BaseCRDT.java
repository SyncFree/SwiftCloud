package swift.crdt;

import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.Timestamp;
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
    public void merge(CRDT<V> otherObject) {
        // FIXME: it's more involved than that!
        // if
        // (updatesClock.compareTo(otherObject.getPruneClock()).is(CMP_CLOCK.CMP_CONCURRENT,
        // CMP_CLOCK.CMP_ISDOMINATED)) {
        // throw new IllegalStateException(
        // "Cannot merge with an object version that pruned concurrently with or later to this version");
        // }
        // if
        // (otherObject.getClock().compareTo(pruneClock).is(CMP_CLOCK.CMP_CONCURRENT,
        // CMP_CLOCK.CMP_ISDOMINATED)) {
        // throw new IllegalStateException(
        // "Cannot merge with an object version lower or concurrent with pruning point of this version");
        // }

        mergePayload((V) otherObject);
        getClock().merge(otherObject.getClock());
        pruneClock.merge(otherObject.getPruneClock());
        registeredInStore |= otherObject.isRegisteredInStore();
    }

    /**
     * Merges the payload of the object with the payload of another object
     * having the same CRDT type.
     * 
     * @param otherObject
     *            object to merge with
     */
    protected abstract void mergePayload(V otherObject);

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
        }

        // Otherwise: IGNORE
        if (!updatesClock.record(ops.getBaseTimestamp())) {
            // Operations group is already included in the state.
            return false;
        }

        if (ops.hasCreationState()) {
            registeredInStore = true;
        }

        for (final CRDTUpdate<V> op : ops.getOperations()) {
            execute(op);
        }
        return true;
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
    public Set<Timestamp> getUpdateTimestampsSince(CausalityClock clock) {
        final CMP_CLOCK pruneCmp = clock.compareTo(pruneClock);
        if (pruneCmp == CMP_CLOCK.CMP_CONCURRENT || pruneCmp == CMP_CLOCK.CMP_ISDOMINATED) {
            throw new IllegalArgumentException();
        }
        return getUpdateTimestampsSinceImpl(clock);
    }

    /**
     * Finds all operations that have been performed strictly after clock on the
     * object.
     * 
     * @param clock
     *            reference time
     * @return set of timestamps for operations that have been performed since
     *         clock
     */
    protected abstract Set<Timestamp> getUpdateTimestampsSinceImpl(CausalityClock clock);

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
        object.init(id, updatesClock.clone(), pruneClock.clone(), registeredInStore);
    }
}
