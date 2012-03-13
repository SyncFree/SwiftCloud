package swift.crdt.operations;

import swift.clocks.TripleTimestamp;

public class IntegerSub extends BaseOperation {
    private int val;

    public IntegerSub(TripleTimestamp ts, int val) {
        super(ts);
        this.val = val;
    }

    public int getVal() {
        return this.val;
    }

}
