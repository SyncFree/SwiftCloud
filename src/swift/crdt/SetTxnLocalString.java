package swift.crdt;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.SetInsert;
import swift.crdt.operations.SetRemove;

public class SetTxnLocalString extends BaseCRDTTxnLocal<SetStrings> {
    private Map<String, Set<TripleTimestamp>> elems;

    public SetTxnLocalString(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, SetStrings creationState,
            Map<String, Set<TripleTimestamp>> elems) {
        super(id, txn, clock, creationState);
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
        registerLocalOperation(new SetInsert<String, SetStrings>(ts, e));
    }

    /**
     * Remove element e from the set.
     * 
     * @param e
     */
    public void remove(String e) {
        Set<TripleTimestamp> ids = elems.remove(e);
        if (ids != null) {
            TripleTimestamp ts = nextTimestamp();
            registerLocalOperation(new SetRemove<String, SetStrings>(ts, e, ids));
        }
    }

    public boolean lookup(String e) {
        return elems.containsKey(e);
    }

    public Set<String> getValue() {
        return elems.keySet();
    }

    @Override
    public Object executeQuery(CRDTQuery<SetStrings> query) {
        return query.executeAt(this);
    }
}
