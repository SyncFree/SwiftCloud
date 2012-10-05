package loria.swift.crdt;

import java.util.Collection;
import java.util.Map;
import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.BaseCRDTTxnLocal;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.Copyable;
import swift.crdt.interfaces.TxnHandle;
import loria.swift.crdt.operation.MultiVersionUpdate;

public class MultiVersionTxnLocal<V extends Copyable> extends BaseCRDTTxnLocal<MultiVersionVersionned<V>> {
    private Map<TripleTimestamp, V> vals;

    public MultiVersionTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, MultiVersionVersionned<V> creationState,
            Map<TripleTimestamp, V> vals) {
        super(id, txn, clock, creationState);
        this.vals = vals;
    }

    public void set(V v) {
        vals.clear();
        TripleTimestamp ts = nextTimestamp();
        vals.put(ts, v);
        registerLocalOperation(new MultiVersionUpdate<V>(vals.keySet(), ts, v, getClock().clone()));
    }

    @Override
    public Collection<V> getValue() {
        return vals.values();
    }

    @Override
    public Object executeQuery(CRDTQuery<MultiVersionVersionned<V>> query) {
        return query.executeAt(this);
    }
}
