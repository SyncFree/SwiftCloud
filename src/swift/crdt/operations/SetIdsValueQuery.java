package swift.crdt.operations;

import java.util.Set;

import swift.crdt.CRDTIdentifier;
import swift.crdt.SetIds;
import swift.crdt.SetTxnLocalId;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SetIdsValueQuery implements CRDTQuery<SetIds> {

    @Override
    public Set<CRDTIdentifier> executeAt(TxnLocalCRDT<SetIds> crdtVersion) {
        // TODO: change TxnLocalCRDT<V> to TxnLocalCRDT<Y, V> to avoid casting?
        return ((SetTxnLocalId) crdtVersion).getValue();
    }
}
