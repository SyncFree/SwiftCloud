package swift.crdt.operations;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDTOperation;

public class IntegerAdd extends BaseOperation {
    private int val;

    public IntegerAdd(CRDTIdentifier target, TripleTimestamp ts, CausalityClock c, int val) {
        super(target, ts, c);
        this.val = val;
    }

    public int getVal() {
        return this.val;
    }

}
