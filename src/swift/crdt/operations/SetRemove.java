package swift.crdt.operations;

import java.util.Set;

import swift.clocks.TripleTimestamp;

public class SetRemove<V> extends BaseOperation implements SetOperation<V> {
    private V val;
    private Set<TripleTimestamp> ids;

    public SetRemove(TripleTimestamp ts, V val, Set<TripleTimestamp> ids) {
        super(ts);
        this.val = val;
        this.ids = ids;
    }

    public V getVal() {
        return this.val;
    }

    public Set<TripleTimestamp> getIds() {
        return this.ids;
    }
}
