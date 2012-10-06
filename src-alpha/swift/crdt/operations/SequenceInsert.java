package swift.crdt.operations;

import swift.clocks.TripleTimestamp;
import swift.crdt.SortedSetVersioned;

public class SequenceInsert<V extends Comparable<V>, T extends SortedSetVersioned<V, T>> extends BaseUpdate<T> {
    private V val;

    // required for kryo
    SequenceInsert() {
    }

    public SequenceInsert(TripleTimestamp ts, V val) {
        super(ts);
        this.val = val;
    }

    public V getVal() {
        return this.val;
    }

    @Override
    public void applyTo(T crdt) {
        crdt.insertU(val, getTimestamp());
    }
}
