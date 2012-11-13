package swift.crdt.operations;

import java.util.Set;

import swift.crdt.SetIntegers;
import swift.crdt.SetTxnLocalInteger;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SetIntegersValueQuery implements CRDTQuery<SetIntegers> {

    @Override
    public Set<Integer> executeAt(TxnLocalCRDT<SetIntegers> crdtVersion) {
        // TODO: change TxnLocalCRDT<V> to TxnLocalCRDT<Y, V> to avoid casting?
        return ((SetTxnLocalInteger) crdtVersion).getValue();
    }
}
