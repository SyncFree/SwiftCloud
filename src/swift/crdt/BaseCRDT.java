package swift.crdt;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public abstract class BaseCRDT<V extends BaseCRDT<V>> implements CRDT<V> {
    private static final long serialVersionUID = 1L;
    private transient CausalityClock updatesClock;
    private transient CausalityClock pruneClock;
    protected transient CRDTIdentifier id;

    public CausalityClock getClock() {
        return updatesClock;
    }

    @Override
    public void setClock(CausalityClock c) {
        this.updatesClock = c;
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
        assertGreaterEqualsPruneClock(pruningPoint);
        pruneClock.merge(pruningPoint);
        pruneImpl(pruningPoint);
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

    @Override
    public void executeOperation(CRDTOperation<V> op) {
        executeImpl(op);
        getClock().record(op.getTimestamp());
    }

    // TODO Use Visitor pattern to dispatch on operation in the respective
    // classes!
    protected abstract void executeImpl(CRDTOperation<V> op);

    @Override
    public CRDTIdentifier getUID() {
        return this.id;
    }

    @Override
    public void setUID(CRDTIdentifier id) {
        this.id = id;
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
            throw new IllegalArgumentException("provided clock is not higher or equal to the prune clock");
        }
    }

}
