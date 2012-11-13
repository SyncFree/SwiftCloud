package swift.crdt.operations;

import swift.crdt.SetIntegers;
import swift.crdt.SetTxnLocalInteger;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SetIntegersLookupQuery implements CRDTQuery<SetIntegers> {
    protected Integer e;

    public SetIntegersLookupQuery(Integer e) {
        this.e = e;
    }

    @Override
    public Boolean executeAt(TxnLocalCRDT<SetIntegers> crdtVersion) {
        // TODO: change TxnLocalCRDT<V> to TxnLocalCRDT<Y, V> to avoid casting?
        return ((SetTxnLocalInteger) crdtVersion).lookup(e);
    }
}
