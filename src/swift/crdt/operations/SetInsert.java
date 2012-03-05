package swift.crdt.operations;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;

public class SetInsert<V> extends BaseOperation implements SetOperation<V> {
    private V val;

    public SetInsert(CRDTIdentifier target, TripleTimestamp ts, CausalityClock c, V val) {
        super(target, ts, c);
        this.val = val;
    }

    public V getVal() {
        return this.val;
    }

}
