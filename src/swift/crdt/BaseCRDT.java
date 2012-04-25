package swift.crdt;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.CRDTOperationDependencyPolicy;
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
        registeredInStore |= otherObject.isRegisteredInStore();
        // pruneClock is preserved
        // FIXME: if otherObject has non-versioned updates that we do not have
        // in verisoned form, we must raise the pruneClock :-(
    }

    protected abstract void mergePayload(V otherObject);

    @Override
    public boolean execute(CRDTObjectOperationsGroup<V> ops, final CRDTOperationDependencyPolicy dependenciesPolicy) {
        if (pruneClock.includes(ops.getBaseTimestamp())) {
            throw new IllegalStateException("Operations group origin prior to the pruning point");
        }
        final CausalityClock dependencyClock = ops.getDependency();
        if (dependenciesPolicy == CRDTOperationDependencyPolicy.CHECK) {
            final CMP_CLOCK dependencyCmp = updatesClock.compareTo(dependencyClock);
            if (dependencyCmp == CMP_CLOCK.CMP_ISDOMINATED || dependencyCmp == CMP_CLOCK.CMP_CONCURRENT) {
                throw new IllegalStateException("Object does not meet operation's dependencies");
            }
        } else if (dependenciesPolicy == CRDTOperationDependencyPolicy.RECORD_BLINDLY) {
            updatesClock.merge(dependencyClock);
        }
        if (!updatesClock.record(ops.getBaseTimestamp())) {
            // Operations group is already included in the state.
            return false;
        }

        if (ops.hasCreationState()) {
            registeredInStore = true;
        }

        for (final CRDTOperation<V> op : ops.getOperations()) {
            execute(op);
        }
        updatesClock.record(ops.getBaseTimestamp());
        return true;
    }

    protected abstract void execute(CRDTOperation<V> op);

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
    public boolean hasUpdatesSince(CausalityClock clock) {
        final CMP_CLOCK clockCmp = clock.compareTo(updatesClock);
        final CMP_CLOCK pruneCmp = clock.compareTo(pruneClock);
        if (clockCmp == CMP_CLOCK.CMP_CONCURRENT || clockCmp == CMP_CLOCK.CMP_ISDOMINATED
                || pruneCmp == CMP_CLOCK.CMP_CONCURRENT || pruneCmp == CMP_CLOCK.CMP_DOMINATES) {
            throw new IllegalArgumentException();
        }
        return hasUpdatesSinceImpl(clock);
    }

    protected abstract boolean hasUpdatesSinceImpl(CausalityClock clock);

    // TODO Implement copy mechanisms for each CRDT!
    public V copy() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(this);
            oos.flush();
            oos.close();
            bos.close();
            byte[] byteData = bos.toByteArray();

            ByteArrayInputStream bais = new ByteArrayInputStream(byteData);
            @SuppressWarnings("unchecked")
            V object = (V) new ObjectInputStream(bais).readObject();
            object.init(id, updatesClock.clone(), pruneClock.clone(), registeredInStore);
            return object;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // this should not happen!
            e.printStackTrace();
        }
        return null;
    }
}
