package swift.crdt;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

public abstract class BaseCRDTTxnLocal<V extends CRDT<V>> implements TxnLocalCRDT<V> {
    private final TxnHandle txn;
    private final CRDTIdentifier id;
    private final CausalityClock clock;

    public BaseCRDTTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, V creationState) {
        this.txn = txn;
        this.id = id;
        this.clock = clock;
        if (creationState != null) {
            txn.registerObjectCreation(this.id, creationState);
        }
    }

    @Override
    public TxnHandle getTxnHandle() {
        return this.txn;
    }

    protected TripleTimestamp nextTimestamp() {
        return getTxnHandle().nextTimestamp();
    }

    protected void registerLocalOperation(final CRDTOperation<V> op) {
        getTxnHandle().registerOperation(this.id, op);
    }

    /**
     * @return constant snapshot clock of this local object representation
     */
    protected CausalityClock getClock() {
        return clock;
    }
}
