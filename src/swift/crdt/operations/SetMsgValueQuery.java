package swift.crdt.operations;

import java.util.Set;

import swift.application.social.Message;
import swift.crdt.SetMsg;
import swift.crdt.SetTxnLocalMsg;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SetMsgValueQuery implements CRDTQuery<SetMsg> {

    @Override
    public Set<Message> executeAt(TxnLocalCRDT<SetMsg> crdtVersion) {
        // TODO: change TxnLocalCRDT<V> to TxnLocalCRDT<Y, V> to avoid casting?
        return ((SetTxnLocalMsg) crdtVersion).getValue();
    }
}
