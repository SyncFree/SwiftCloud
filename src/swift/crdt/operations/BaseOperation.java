package swift.crdt.operations;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;

public abstract class BaseOperation<V extends CRDT<V>> implements CRDTOperation<V> {
    private TripleTimestamp ts;

    // required by kryo
    protected BaseOperation() {
    }

    protected BaseOperation(TripleTimestamp ts) {
        this.ts = ts;
    }

    @Override
    public TripleTimestamp getTimestamp() {
        return this.ts;
    }

    @Override
    public void replaceBaseTimestamp(Timestamp newBaseTimestamp) {
        ts = ts.withBaseTimestamp(newBaseTimestamp);
    }

    @Override
    public boolean hasCreationState() {
        return false;
    }

    @Override
    public V getCreationState() {
        return null;
    }
}
