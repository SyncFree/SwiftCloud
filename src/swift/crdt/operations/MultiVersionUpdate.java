package swift.crdt.operations;

import java.util.Collection;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.MultiVersionVersionned;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.Copyable;

public class MultiVersionUpdate<V extends Copyable> extends BaseUpdate<MultiVersionVersionned<V>> {
    private V val;
    private CausalityClock c;
    private Collection<TripleTimestamp> olds;
    

    // required for kryo
    public MultiVersionUpdate() {
    }

    public MultiVersionUpdate(Collection<TripleTimestamp> olds, TripleTimestamp ts, 
            V val, CausalityClock c) {
        super(ts);
        this.olds = olds;
        this.val = val;
        this.c = c;
    }

    public V getVal() {
        return this.val;
    }

    @Override
    public void replaceDependeeOperationTimestamp(Timestamp oldTs, Timestamp newTs) {
        // WISHME: extract as a CausalityClock method?
        if (c.includes(oldTs)) {
            c.drop(oldTs);
            c.record(newTs);
        }
    }

    @Override
    public void applyTo(MultiVersionVersionned<V> register) {
        register.update(olds, val, getTimestamp(), c);
    }

    @Override
    public CRDTUpdate<MultiVersionVersionned<V>> withBaseTimestamp(Timestamp ts) {
        return new MultiVersionUpdate<V>(olds, getTimestamp().withBaseTimestamp(ts), val, c);
    }
}
