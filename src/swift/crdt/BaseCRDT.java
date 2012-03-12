package swift.crdt;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.TxnHandle;

public abstract class BaseCRDT<V extends BaseCRDT<V>> implements CRDT<V> {
    private static final long serialVersionUID = 1L;
    private transient CausalityClock clock;
    private transient CRDTIdentifier id;
    private transient TxnHandle txn;

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

    protected TripleTimestamp nextTimestamp() {
        return getTxnHandle().nextTimestamp();
    }

    protected void registerLocalOperation(final CRDTOperation op) {
        executeOperation(op);
        getTxnHandle().registerOperation(op);
    }

    @Override
    public CRDTIdentifier getUID() {
        return this.id;
    }

    @Override
    public void setUID(CRDTIdentifier id) {
        this.id = id;
    }

    @Override
    public TxnHandle getTxnHandle() {
        return this.txn;
    }

    @Override
    public void setTxnHandle(TxnHandle txn) {
        this.txn = txn;
    }
}
