package swift.client;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.TxnHandle;

public class TxnHandleImpl implements TxnHandle {

    @Override
    public <V extends CRDT<V, I>, I extends CRDTOperation> V get(CRDTIdentifier id, boolean create, Class<V> classOfT) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void commit() {
        // TODO Auto-generated method stub

    }

    @Override
    public void rollback() {
        // TODO Auto-generated method stub

    }

    @Override
    public TripleTimestamp nextTimestamp() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CausalityClock getSnapshotClock() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <I extends CRDTOperation> void registerOperation(I op) {
        // TODO Auto-generated method stub

    }

}
