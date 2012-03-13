package swift.crdt.operations;

import swift.clocks.TripleTimestamp;

public class IntegerAdd extends BaseOperation {
    private int val;

    public IntegerAdd(TripleTimestamp ts, int val) {
        super(ts);
        this.val = val;
    }

    public int getVal() {
        return this.val;
    }

}
