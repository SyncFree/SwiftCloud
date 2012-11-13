package swift.crdt.operations;

import swift.clocks.TimestampMapping;
import swift.clocks.TripleTimestamp;
import swift.crdt.BaseCRDT;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTUpdate;

public abstract class BaseUpdate<V extends CRDT<V>> implements CRDTUpdate<V> {
    private TripleTimestamp ts;

    // required by kryo
    protected BaseUpdate() {
    }

    protected BaseUpdate(TripleTimestamp ts) {
        this.ts = ts;
    }

    @Override
    public TripleTimestamp getTimestamp() {
        return this.ts;
    }

    @Override
    public void setTimestampMapping(final TimestampMapping mapping) {
        ts = ts.copyWithMappings(mapping);
    }

    /**
     * Applies operation to the given object instance. Importantly, any newly
     * used timestamp mapping should be registered through in
     * {@link BaseCRDT#registerTimestampUsage(TripleTimestamp)}.
     * 
     * @param crdt
     *            object where operation is applied
     */
    public abstract void applyTo(V crdt);
}
