package swift.crdt.operations;

import swift.clocks.TripleTimestamp;

public class SetInsert<V> extends BaseOperation implements SetOperation<V> {
    private V val;

    public SetInsert(TripleTimestamp ts, V val) {
        super(ts);
        this.val = val;
    }

    public V getVal() {
        return this.val;
    }

}
