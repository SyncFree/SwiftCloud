package swift.crdt.operations;

import swift.crdt.RegisterTxnLocal;
import swift.crdt.RegisterVersioned;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.Copyable;
import swift.crdt.interfaces.TxnLocalCRDT;

public class RegisterValueQuery<V extends Copyable> implements CRDTQuery<RegisterVersioned<V>> {

    @Override
    public V executeAt(TxnLocalCRDT<RegisterVersioned<V>> crdtVersion) {
        // TODO: change TxnLocalCRDT<V> to TxnLocalCRDT<Y, V> to avoid casting?
        return ((RegisterTxnLocal<V>) crdtVersion).getValue();
    }
}
