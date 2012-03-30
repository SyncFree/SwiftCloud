package swift.crdt.operations;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.SetVersioned;
import swift.crdt.interfaces.CRDTOperation;

public class SetInsert<V, T extends SetVersioned<V, T>> extends BaseOperation<T> {
    private V val;

    public SetInsert(TripleTimestamp ts, V val) {
        super(ts);
        this.val = val;
    }

    public V getVal() {
        return this.val;
    }

    @Override
    public void replaceDependentOpTimestamp(Timestamp oldTs, Timestamp newTs) {
        // Insert does not rely on any timestamp dependency.
    }

    @Override
    public void applyTo(T crdt) {
        crdt.insertU(val, getTimestamp());
    }

    @Override
    public CRDTOperation<T> withBaseTimestamp(Timestamp ts) {
        return new SetInsert<V, T>(getTimestamp().withBaseTimestamp(ts), val);
    }
}
