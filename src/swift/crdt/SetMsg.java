package swift.crdt;

import swift.application.social.Message;
import swift.clocks.CausalityClock;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SetMsg extends SetVersioned<Message, SetMsg> {

    private static final long serialVersionUID = 1L;

    public SetMsg() {
    }

    @Override
    protected TxnLocalCRDT<SetMsg> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        final SetMsg creationState = isRegisteredInStore() ? null : new SetMsg();
        SetTxnLocalMsg localView = new SetTxnLocalMsg(id, txn, versionClock, creationState, getValue(versionClock));
        return localView;
    }

    @Override
    protected void execute(CRDTUpdate<SetMsg> op) {
        op.applyTo(this);
    }

    @Override
    public SetMsg copy() {
        SetMsg copy = new SetMsg();
        copyLoad(copy);
        copyBase(copy);
        return copy;
    }
}
