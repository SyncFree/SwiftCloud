package swift.crdt.operations;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;

public class IntegerUpdate extends BaseOperation {
    private int val;

    public IntegerUpdate(TripleTimestamp ts, int val) {
        super(ts);
        this.val = val;
    }

    public int getVal() {
        return this.val;
    }

    @Override
    public void replaceDependentOpTimestamp(Timestamp oldTs, Timestamp newTs) {
        // Integer operation does not rely on any timestamp dependency.
    }

}
