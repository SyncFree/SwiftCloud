package swift.crdt.operations;

import swift.crdt.CRDTIdentifier;
import swift.crdt.SetIds;
import swift.crdt.SetTxnLocalId;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnLocalCRDT;

public class SetIdsLookupQuery implements CRDTQuery<SetIds> {
    protected CRDTIdentifier e;

    public SetIdsLookupQuery(CRDTIdentifier e) {
        this.e = e;
    }

    @Override
    public Boolean executeAt(TxnLocalCRDT<SetIds> crdtVersion) {
        // TODO: change TxnLocalCRDT<V> to TxnLocalCRDT<Y, V> to avoid casting?
        return ((SetTxnLocalId) crdtVersion).lookup(e);
    }
}
