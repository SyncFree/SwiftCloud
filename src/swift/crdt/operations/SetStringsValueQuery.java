package swift.crdt.operations;

import java.util.Set;

import swift.crdt.SetStrings;
import swift.crdt.SetTxnLocalString;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SetStringsValueQuery implements CRDTQuery<SetStrings> {

    @Override
    public Set<String> executeAt(TxnLocalCRDT<SetStrings> crdtVersion) {
        // TODO: change TxnLocalCRDT<V> to TxnLocalCRDT<Y, V> to avoid casting?
        return ((SetTxnLocalString) crdtVersion).getValue();
    }
}
