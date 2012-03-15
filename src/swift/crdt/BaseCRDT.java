package swift.crdt;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;

public abstract class BaseCRDT<V extends BaseCRDT<V>> implements CRDT<V> {
    private static final long serialVersionUID = 1L;
    private transient CausalityClock clock;
    private transient CausalityClock pruneClock;
    protected transient CRDTIdentifier id;

    public CausalityClock getClock() {
        return clock;
    }

    @Override
    public void setClock(CausalityClock c) {
        this.clock = c;
    }

    public CausalityClock getPruneClock() {
        return pruneClock;
    }

    @Override
    public void setPruneClock(CausalityClock c) {
        this.pruneClock = c;
    }

    @Override
    public void prune(CausalityClock pruningPoint) {
        final CMP_CLOCK clockCmp = getPruneClock().compareTo(pruningPoint);
        if (clockCmp == CMP_CLOCK.CMP_EQUALS || clockCmp == CMP_CLOCK.CMP_DOMINATES) {
            throw new IllegalArgumentException("pruning point does not dominate existing prune clock");
        }
        pruneImpl(pruningPoint);
        pruneClock.merge(pruningPoint);
    }

    protected abstract void pruneImpl(CausalityClock pruningPoint);

    public void merge(V otherObject) {
        mergePayload(otherObject);
        getClock().merge(otherObject.getClock());
        // pruneClock is preserved
        // FIXME: if otherObject has non-versioned updates that we do not have
        // in verisoned form, we must raise the pruneClock :-(
    }

    protected abstract void mergePayload(V otherObject);

    public void executeOperation(CRDTOperation op) {
        executeImpl(op);
        getClock().record(op.getTimestamp());
    }

    protected abstract void executeImpl(CRDTOperation op);

    @Override
    public CRDTIdentifier getUID() {
        return this.id;
    }

    @Override
    public void setUID(CRDTIdentifier id) {
        this.id = id;
    }
}
