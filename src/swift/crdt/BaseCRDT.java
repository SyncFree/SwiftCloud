package swift.crdt;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.TxnHandle;
import swift.exceptions.InvalidParameterException;

public abstract class BaseCRDT<V extends BaseCRDT<V, I>, I> implements CRDT<V, I> {
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

    protected void registerUpdate(Timestamp ts) throws InvalidParameterException {
        getClock().record(ts);
    }

    protected abstract void mergePayload(V otherObject);

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
