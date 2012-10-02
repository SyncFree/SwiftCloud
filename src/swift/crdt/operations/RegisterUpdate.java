package swift.crdt.operations;

import swift.clocks.TripleTimestamp;
import swift.crdt.RegisterVersioned;
import swift.crdt.interfaces.Copyable;

public class RegisterUpdate<V extends Copyable> extends BaseUpdate<RegisterVersioned<V>> {
    private V val;
    private long lamportClock;

    // required for kryo
    public RegisterUpdate() {
    }

    public RegisterUpdate(TripleTimestamp ts, long lamportClock, V val) {
        super(ts);
        this.lamportClock = lamportClock;
        this.val = val;
    }

    public V getVal() {
        return this.val;
    }

    @Override
    public void applyTo(RegisterVersioned<V> register) {
        register.update(lamportClock, getTimestamp(), val);
    }
}
