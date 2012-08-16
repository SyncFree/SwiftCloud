package swift.crdt;

import swift.clocks.CausalityClock;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SetStrings extends SetVersioned<String, SetStrings> {

    private static final long serialVersionUID = 1L;

    public SetStrings() {
    }

    @Override
    protected TxnLocalCRDT<SetStrings> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        final SetStrings creationState = isRegisteredInStore() ? null : new SetStrings();
        SetTxnLocalString localView = new SetTxnLocalString(id, txn, versionClock, creationState,
                getValue(versionClock));
        return localView;
    }

    @Override
    protected void execute(CRDTUpdate<SetStrings> op) {
        op.applyTo(this);
    }

    @Override
    public SetStrings copy() {
        SetStrings copy = new SetStrings();
        copyLoad(copy);
        copyBase(copy);
        return copy;
    }
}
