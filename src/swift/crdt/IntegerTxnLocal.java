package swift.crdt;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.IntegerAdd;
import swift.crdt.operations.IntegerSub;

public class IntegerTxnLocal extends BaseCRDTTxnLocal<IntegerVersioned> {
    private int val;

    public IntegerTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, int val) {
        super(id, txn, clock);
        this.val = val;
    }

    public int value() {
        return val;
    }

    public void add(int n) {
        val += n;
        TripleTimestamp ts = nextTimestamp();
        registerLocalOperation(new IntegerAdd(ts, n));
    }

    public void sub(int n) {
        val -= n;
        TripleTimestamp ts = nextTimestamp();
        registerLocalOperation(new IntegerSub(ts, n));
    }

}
