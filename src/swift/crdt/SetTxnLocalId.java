package swift.crdt;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.SetInsert;
import swift.crdt.operations.SetRemove;

public class SetTxnLocalId extends BaseCRDTTxnLocal<SetIds> {
    private Map<CRDTIdentifier, Set<TripleTimestamp>> elems;

    public SetTxnLocalId(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, SetIds creationState,
            Map<CRDTIdentifier, Set<TripleTimestamp>> elems) {
        super(id, txn, clock, creationState);
        this.elems = elems;
    }

    /**
     * Insert element e in the set, using the given unique identifier.
     * 
     * @param e
     */
    public void insert(CRDTIdentifier e) {
        Set<TripleTimestamp> adds = elems.get(e);
        if (adds == null) {
            adds = new HashSet<TripleTimestamp>();
            elems.put(e, adds);
        }

        TripleTimestamp ts = nextTimestamp();
        adds.add(ts);
        registerLocalOperation(new SetInsert<CRDTIdentifier, SetIds>(ts, e));
    }

    /**
     * Remove element e from the set.
     * 
     * @param e
     */
    public void remove(CRDTIdentifier e) {
        Set<TripleTimestamp> ids = elems.remove(e);
        if (ids != null) {
            TripleTimestamp ts = nextTimestamp();
            registerLocalOperation(new SetRemove<CRDTIdentifier, SetIds>(ts, e, ids));
        }
    }

    public boolean lookup(int e) {
        return elems.containsKey(e);
    }

    public Set<CRDTIdentifier> getValue() {
        return elems.keySet();
    }

}