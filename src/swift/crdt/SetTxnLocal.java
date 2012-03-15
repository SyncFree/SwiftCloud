package swift.crdt;

import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.SetInsert;
import swift.crdt.operations.SetRemove;

public class SetTxnLocal<V> extends BaseCRDTTxnLocal<SetVersioned<V>> {
    private Set<V> elems;

    public SetTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock snapshotClock, Set<V> elems) {
        super(id, txn, snapshotClock);
        this.elems = elems;
    }

    /**
     * Insert element V in the set, using the given unique identifier.
     * 
     * @param e
     */
    public void insert(V e) {
        TripleTimestamp ts = nextTimestamp();
        registerLocalOperation(new SetInsert<V>(ts, e));
    }

    /**
     * Remove element e from the set.
     * 
     * @param e
     */
    public void remove(V e) {
        TripleTimestamp ts = nextTimestamp();
        registerLocalOperation(new SetRemove<V>(ts, e));
    }

    public boolean lookup(V e) {
        return elems.contains(e);
    }

    public Set<V> getValue() {
        return elems;
    }

}
