package swift.crdt;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.operations.CRDTObjectOperationsGroup;

public abstract class BaseCRDT<V extends BaseCRDT<V>> implements CRDT<V> {
    private static final long serialVersionUID = 1L;
    private transient CausalityClock updatesClock;
    private transient CausalityClock pruneClock;
    protected transient CRDTIdentifier id;
    protected transient boolean registeredInStore;

    @Override
    public void init(CRDTIdentifier id, CausalityClock clock, CausalityClock pruneClock, boolean registeredInStore) {
        this.id = id;
        this.updatesClock = clock;
        this.pruneClock = pruneClock;
        this.registeredInStore = registeredInStore;
    }

    public CausalityClock getClock() {
        return updatesClock;
    }

    public CausalityClock getPruneClock() {
        return pruneClock;
    }

    @Override
    public void prune(CausalityClock pruningPoint) {
        assertGreaterEqualsPruneClock(pruningPoint);
        pruneClock.merge(pruningPoint);
        pruneImpl(pruningPoint);
    }

    protected abstract void pruneImpl(CausalityClock pruningPoint);

    public void merge(CRDT<V> otherObject) {
        mergePayload((V) otherObject);
        getClock().merge(otherObject.getClock());
        // pruneClock is preserved
        // FIXME: if otherObject has non-versioned updates that we do not have
        // in verisoned form, we must raise the pruneClock :-(
    }

    protected abstract void mergePayload(V otherObject);

    @Override
    public boolean execute(CRDTObjectOperationsGroup<V> ops, boolean checkDependency) {
        if (pruneClock.includes(ops.getBaseTimestamp())) {
            throw new IllegalStateException("Operations group origin prior to the pruning point");
        }
        final CausalityClock dependencyClock = ops.getDependency();
        if (checkDependency) {
            final CMP_CLOCK dependencyCmp = updatesClock.compareTo(dependencyClock);
            if (dependencyCmp == CMP_CLOCK.CMP_ISDOMINATED || dependencyCmp == CMP_CLOCK.CMP_CONCURRENT) {
                throw new IllegalStateException("Object does not meet operation's dependencies");
            }
        } else {
            // TODO: Discuss this approach.
            updatesClock.merge(dependencyClock);
        }
        if (!updatesClock.record(ops.getBaseTimestamp())) {
            // Operations group is already included in the state.
            return false;
        }

        for (final CRDTOperation<V> op : ops.getOperations()) {
            // TODO: either leave this cast as is (same as in merge()) or use
            // an indirection method execute(CRDTOperation<V>) to avoid it.
            op.applyTo((V) this);
        }
        updatesClock.record(ops.getBaseTimestamp());
        return true;
    }

    @Override
    public CRDTIdentifier getUID() {
        return this.id;
    }

    public TxnLocalCRDT<V> getTxnLocalCopy(CausalityClock versionClock, TxnHandle txn) {
        assertGreaterEqualsPruneClock(versionClock);
        return getTxnLocalCopyImpl(versionClock, txn);
    }

    protected abstract TxnLocalCRDT<V> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn);

    protected void assertGreaterEqualsPruneClock(CausalityClock clock) {
        if (getPruneClock() == null) {
            return;
        }
        final CMP_CLOCK clockCmp = getPruneClock().compareTo(clock);
        if (clockCmp == CMP_CLOCK.CMP_CONCURRENT || clockCmp == CMP_CLOCK.CMP_DOMINATES) {
            throw new IllegalStateException("provided clock is not higher or equal to the prune clock");
        }
    }

    protected void assertPruneClockWithoutExpceptions(CausalityClock clock) {
        if (clock.hasExceptions()) {
            throw new IllegalArgumentException("provided clock has exceptions and cannot be used as prune clock");
        }
    }

    @Override
    public boolean isRegisteredInStore() {
        return registeredInStore;
    }

    @Override
    public void markRegisteredInStore() {
        this.registeredInStore = true;
    }

    @Override
    public abstract V clone();
}
