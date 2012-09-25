package swift.crdt.operations;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.IntegerVersioned;
import swift.crdt.interfaces.CRDTUpdate;

public class IntegerUpdate extends BaseUpdate<IntegerVersioned> {
    private int val;

    // required for kryo
    public IntegerUpdate() {
    }

    public IntegerUpdate(TripleTimestamp ts, int val) {
        super(ts);
        this.val = val;
    }

    public int getVal() {
        return this.val;
    }

    @Override
    public void applyTo(IntegerVersioned crdt) {
        crdt.applyUpdate(val, getTimestamp());
    }
}
