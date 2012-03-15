package swift.crdt;

import swift.clocks.CausalityClock;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;

public abstract class BaseCRDT<V extends BaseCRDT<V>> implements CRDT<V> {
    private static final long serialVersionUID = 1L;
    private transient CausalityClock clock;
    protected transient CRDTIdentifier id;

    public CausalityClock getClock() {
        return clock;
    }

    @Override
    public void setClock(CausalityClock c) {
        this.clock = c;
    }

    public void merge(V otherObject) {
        mergePayload(otherObject);
        getClock().merge(otherObject.getClock());
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
