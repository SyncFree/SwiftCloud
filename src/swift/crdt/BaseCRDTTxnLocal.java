package swift.crdt;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public abstract class BaseCRDTTxnLocal<V> implements TxnLocalCRDT<V> {
    private final TxnHandle txn;
    private final CausalityClock snapshotClock;
    private final CRDTIdentifier id;

    public BaseCRDTTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock snapshotClock) {
        this.txn = txn;
        this.snapshotClock = snapshotClock;
        this.id = id;
    }

    public TxnHandle getTxnHandle() {
        return this.txn;
    }

    protected TripleTimestamp nextTimestamp() {
        return getTxnHandle().nextTimestamp();
    }

    protected CausalityClock getSnapshotClock() {
        return this.snapshotClock;
    }

    protected void registerLocalOperation(final CRDTOperation op) {
        getTxnHandle().registerOperation(this.id, op);
    }

}
