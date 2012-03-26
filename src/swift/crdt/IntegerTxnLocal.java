package swift.crdt;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.IntegerUpdate;

public class IntegerTxnLocal extends BaseCRDTTxnLocal<IntegerVersioned> {
    private int val;

    public IntegerTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, boolean registeredInStore, int val) {
        super(id, txn, clock, registeredInStore);
        this.val = val;
    }

    public int value() {
        return val;
    }

    public void add(int n) {
        val += n;
        TripleTimestamp ts = nextTimestamp();
        registerLocalOperation(new IntegerUpdate(ts, n));
    }

    public void sub(int n) {
        add(-n);
    }

}
