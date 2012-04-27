package swift.crdt;

import swift.clocks.CausalityClock;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SetIds extends SetVersioned<CRDTIdentifier, SetIds> {

    private static final long serialVersionUID = 1L;

    public SetIds() {
    }

    @Override
    protected TxnLocalCRDT<SetIds> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        final SetIds creationState = isRegisteredInStore() ? null : new SetIds();
        SetTxnLocalId localView = new SetTxnLocalId(id, txn, versionClock, creationState, getValue(versionClock));
        return localView;
    }

    @Override
    protected void execute(CRDTOperation<SetIds> op) {
        op.applyTo(this);
    }

    @Override
    public SetIds copy() {
        SetIds copy = new SetIds();
        copyLoad(copy);
        copyBase(copy);
        return copy;
    }
}
