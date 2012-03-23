package swift.crdt;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.SetInsert;
import swift.crdt.operations.SetRemove;

public class SetTxnLocalString extends BaseCRDTTxnLocal<SetStrings> {
    private Map<String, Set<TripleTimestamp>> elems;

    public SetTxnLocalString(CRDTIdentifier id, TxnHandle txn, CausalityClock snapshotClock,
            Map<String, Set<TripleTimestamp>> elems) {
        super(id, txn, snapshotClock);
        this.elems = elems;
    }

    /**
     * Insert element e in the set, using the given unique identifier.
     * 
     * @param e
     */
    public void insert(String e) {
        Set<TripleTimestamp> adds = elems.get(e);
        if (adds == null) {
            adds = new HashSet<TripleTimestamp>();
            elems.put(e, adds);
        }

        TripleTimestamp ts = nextTimestamp();
        adds.add(ts);
        registerLocalOperation(new SetInsert<String>(ts, e));
    }

    /**
     * Remove element e from the set.
     * 
     * @param e
     */
    public void remove(String e) {
        Set<TripleTimestamp> ids = elems.remove(e);
        TripleTimestamp ts = nextTimestamp();
        registerLocalOperation(new SetRemove<String>(ts, e, ids));
    }

    public boolean lookup(int e) {
        return elems.containsKey(e);
    }

    public Set<String> getValue() {
        return elems.keySet();
    }

}
