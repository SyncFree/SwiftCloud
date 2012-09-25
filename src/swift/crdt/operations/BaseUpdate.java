package swift.crdt.operations;

import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTUpdate;

public abstract class BaseUpdate<V extends CRDT<V>> implements CRDTUpdate<V> {
    private TripleTimestamp ts;

    // required by kryo
    protected BaseUpdate() {
    }

    protected BaseUpdate(TripleTimestamp ts) {
        this.ts = ts.copy();
    }

    @Override
    public TripleTimestamp getTimestamp() {
        return this.ts;
    }

    @Override
    public void setTimestampMapping(TimestampMapping mapping) {
        ts.setMapping(mapping);
    }
}
