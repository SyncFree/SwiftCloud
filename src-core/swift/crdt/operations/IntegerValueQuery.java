package swift.crdt.operations;

import swift.crdt.IntegerTxnLocal;
import swift.crdt.IntegerVersioned;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnLocalCRDT;

public class IntegerValueQuery implements CRDTQuery<IntegerVersioned> {

    @Override
    public Integer executeAt(TxnLocalCRDT<IntegerVersioned> crdtVersion) {
        // TODO: change TxnLocalCRDT<V> to TxnLocalCRDT<Y, V> to avoid casting?
        return ((IntegerTxnLocal) crdtVersion).getValue();
    }
}
