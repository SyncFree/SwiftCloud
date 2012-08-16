package swift.crdt;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.Copyable;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.RegisterUpdate;

public class RegisterTxnLocal<V extends Copyable> extends BaseCRDTTxnLocal<RegisterVersioned<V>> {
    private V val;

    public RegisterTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, RegisterVersioned<V> creationState,
            V val) {
        super(id, txn, clock, creationState);
        this.val = val;
    }

    public void set(V v) {
        val = v;
        TripleTimestamp ts = nextTimestamp();
        registerLocalOperation(new RegisterUpdate<V>(ts, v, getClock().clone()));
    }

    public V getValue() {
        return val;
    }

    @Override
    public Object executeQuery(CRDTQuery<RegisterVersioned<V>> query) {
        return query.executeAt(this);
    }
}
