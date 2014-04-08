package swift.crdt;

import swift.clocks.CausalityClock;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

public abstract class BoundedCounterCRDT<T extends BoundedCounterCRDT<T>> extends BaseCRDT<T> {

    public BoundedCounterCRDT() {
        super();
    }

    public BoundedCounterCRDT(CRDTIdentifier id) {
        super(id);
    }

    public BoundedCounterCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock) {
        super(id, txn, clock);
    }

    public abstract boolean decrement(int amount, String siteId);

    public abstract boolean increment(int amount, String siteId);

    public abstract boolean transfer(int amount, String originId, String targetId);

    public abstract void applyDec(BoundedCounterDecrement<T> op);

    public abstract void applyInc(BoundedCounterIncrement<T> op);

    public abstract void applyTransfer(BoundedCounterTransfer<T> op);
}
