package swift.crdt.operations;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.RegisterVersioned;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.Copyable;

public class RegisterUpdate<V extends Copyable> extends BaseOperation<RegisterVersioned<V>> {
    private V val;
    private CausalityClock c;

    // required for kryo
    public RegisterUpdate() {
    }

    public RegisterUpdate(TripleTimestamp ts, V val, CausalityClock c) {
        super(ts);
        this.val = val;
        this.c = c;
    }

    public V getVal() {
        return this.val;
    }

    @Override
    public void replaceDependentOpTimestamp(Timestamp oldTs, Timestamp newTs) {
        // WISHME: extract as a CausalityClock method?
        if (c.includes(oldTs)) {
            c.drop(oldTs);
            c.record(newTs);
        }
    }

    @Override
    public void applyTo(RegisterVersioned<V> register) {
        register.update(val, getTimestamp(), c);
    }

    @Override
    public CRDTOperation<RegisterVersioned<V>> withBaseTimestamp(Timestamp ts) {
        return new RegisterUpdate<V>(getTimestamp().withBaseTimestamp(ts), val, c);
    }
}
