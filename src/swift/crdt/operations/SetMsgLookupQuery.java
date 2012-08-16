package swift.crdt.operations;

import swift.application.social.Message;
import swift.crdt.SetMsg;
import swift.crdt.SetTxnLocalMsg;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SetMsgLookupQuery implements CRDTQuery<SetMsg> {
    protected Message e;

    public SetMsgLookupQuery(Message e) {
        this.e = e;
    }

    @Override
    public Boolean executeAt(TxnLocalCRDT<SetMsg> crdtVersion) {
        // TODO: change TxnLocalCRDT<V> to TxnLocalCRDT<Y, V> to avoid casting?
        return ((SetTxnLocalMsg) crdtVersion).lookup(e);
    }
}
