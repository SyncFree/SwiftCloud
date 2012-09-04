package swift.crdt;

import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.Timestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.CRDTOperationDependencyPolicy;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import sys.net.impl.KryoLib;


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

    protected abstract void execute(CRDTUpdate<V> op);

    @Override
    public CRDTIdentifier getUID() {
        return this.id;
    }

    @Override
    public boolean isRegisteredInStore() {
        return registeredInStore;
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
    public Set<Timestamp> getUpdateTimestampsSince(CausalityClock clock) {
        final CMP_CLOCK pruneCmp = clock.compareTo(pruneClock);
        if (pruneCmp == CMP_CLOCK.CMP_CONCURRENT || pruneCmp == CMP_CLOCK.CMP_ISDOMINATED) {
            throw new IllegalArgumentException();
        }
        // TODO: perhaps enforce only Timestamps and not TripleTimestamps?
        return getUpdateTimestampsSinceImpl(clock);
    }

    protected abstract Set<Timestamp> getUpdateTimestampsSinceImpl(CausalityClock clock);

    public V copy() {
//        final Kryo kryo = KryoLib.kryo();
//        final ObjectBuffer objectBuffer = new ObjectBuffer(kryo);
//        final V copy = (V) objectBuffer.readClassAndObject(objectBuffer.writeClassAndObject(this));
        final V copy = (V)KryoLib.copy( this ) ;
        copy.init(id, updatesClock.clone(), pruneClock.clone(), registeredInStore);
        return copy;
    }

    protected void copyBase(V object) {
        object.init(id, updatesClock.clone(), pruneClock.clone(), registeredInStore);
    }
}
