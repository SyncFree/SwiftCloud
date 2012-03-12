package swift.crdt.operations;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;

public class IntegerSub extends BaseOperation {
    private int val;

    public IntegerSub(CRDTIdentifier target, TripleTimestamp ts, CausalityClock c, int val) {
        super(target, ts, c);
        this.val = val;
    }

    public int getVal() {
        return this.val;
    }

}
