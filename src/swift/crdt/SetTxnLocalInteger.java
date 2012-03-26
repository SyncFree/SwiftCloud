package swift.crdt;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.SetInsert;
import swift.crdt.operations.SetRemove;

public class SetTxnLocalInteger extends BaseCRDTTxnLocal<SetIntegers> {
    private Map<Integer, Set<TripleTimestamp>> elems;

    public SetTxnLocalInteger(CRDTIdentifier id, TxnHandle txn, CausalityClock snapshotClock,
            SetIntegers creationState, Map<Integer, Set<TripleTimestamp>> elems) {
        super(id, txn, snapshotClock, creationState);
        this.elems = elems;
    }

    /**
     * Insert element e in the set, using the given unique identifier.
     * 
     * @param e
     */
    public void insert(int e) {
        Set<TripleTimestamp> adds = elems.get(e);
        if (adds == null) {
            adds = new HashSet<TripleTimestamp>();
            elems.put(e, adds);
        }

        TripleTimestamp ts = nextTimestamp();
        adds.add(ts);
        registerLocalOperation(new SetInsert<Integer, SetIntegers>(ts, e));
    }

    /**
     * Remove element e from the set.
     * 
     * @param e
     */
    public void remove(int e) {
        Set<TripleTimestamp> ids = elems.remove(e);
        TripleTimestamp ts = nextTimestamp();
        registerLocalOperation(new SetRemove<Integer, SetIntegers>(ts, e, ids));
    }

    public boolean lookup(int e) {
        return elems.containsKey(e);
    }

    public Set<Integer> getValue() {
        return elems.keySet();
    }

}
