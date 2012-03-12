package swift.client;

import java.util.List;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;

public class SwiftImpl implements Swift {

    @Override
    public TxnHandle beginTxn(CachePolicy cp, boolean readOnly) {
        // TODO Auto-generated method stub
        return null;
    }

    public CRDT<?, ?> getObjectVersion(TxnHandleImpl txnHandleImpl, CRDTIdentifier id, CausalityClock version,
            boolean create) {
        // TODO Auto-generated method stub
        return null;
    }

    public void commitTxn(TxnHandleImpl txnHandleImpl, List<CRDTOperation> operations) {
        // TODO Auto-generated method stub

    }

    public void discardTxn(TxnHandleImpl txnHandleImpl) {
        // TODO Auto-generated method stub

    }

}
