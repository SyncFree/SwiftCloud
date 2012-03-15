package swift.crdt;

import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.SetInsert;
import swift.crdt.operations.SetRemove;

public class SetTxnLocalInteger extends BaseCRDTTxnLocal<SetIntegers> {
    private Set<Integer> elems;

    public SetTxnLocalInteger(CRDTIdentifier id, TxnHandle txn, CausalityClock snapshotClock, Set<Integer> elems) {
        super(id, txn, snapshotClock);
        this.elems = elems;
    }

    /**
     * Insert element e in the set, using the given unique identifier.
     * 
     * @param e
     */
    public void insert(int e) {
        elems.add(e);
        TripleTimestamp ts = nextTimestamp();
        registerLocalOperation(new SetInsert<Integer>(ts, e));
    }

    /**
     * Remove element e from the set.
     * 
     * @param e
     */
    public void remove(int e) {
        elems.remove(e);
        TripleTimestamp ts = nextTimestamp();
        registerLocalOperation(new SetRemove<Integer>(ts, e));
    }

    public boolean lookup(int e) {
        return elems.contains(e);
    }

    public Set<Integer> getValue() {
        return elems;
    }

}
