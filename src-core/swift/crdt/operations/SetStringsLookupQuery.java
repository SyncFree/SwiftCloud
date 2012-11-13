package swift.crdt.operations;

import swift.crdt.SetStrings;
import swift.crdt.SetTxnLocalString;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SetStringsLookupQuery implements CRDTQuery<SetStrings> {
    protected String e;

    public SetStringsLookupQuery(String e) {
        this.e = e;
    }

    @Override
    public Boolean executeAt(TxnLocalCRDT<SetStrings> crdtVersion) {
        // TODO: change TxnLocalCRDT<V> to TxnLocalCRDT<Y, V> to avoid casting?
        return ((SetTxnLocalString) crdtVersion).lookup(e);
    }
}
