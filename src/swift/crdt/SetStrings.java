package swift.crdt;

import swift.clocks.CausalityClock;
import swift.crdt.interfaces.CRDTOperation;
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
        return (TxnLocalCRDT<SetStrings>) localView;
    }

    @Override
    protected void executeImpl(CRDTOperation<SetStrings> op) {
        op.applyTo(this);
    }

}
