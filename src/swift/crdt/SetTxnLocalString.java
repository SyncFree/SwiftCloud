package swift.crdt;

import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.SetInsert;
import swift.crdt.operations.SetRemove;

public class SetTxnLocalString extends BaseCRDTTxnLocal<SetStrings> {
    private Set<String> elems;

    public SetTxnLocalString(CRDTIdentifier id, TxnHandle txn, CausalityClock snapshotClock, Set<String> elems) {
        super(id, txn, snapshotClock);
        this.elems = elems;
    }

    /**
     * Insert element e in the set, using the given unique identifier.
     * 
     * @param e
     */
    public void insert(String e) {
        elems.add(e);
        TripleTimestamp ts = nextTimestamp();
        registerLocalOperation(new SetInsert<String>(ts, e));
    }

    /**
     * Remove element e from the set.
     * 
     * @param e
     */
    public void remove(String e) {
        elems.remove(e);
        TripleTimestamp ts = nextTimestamp();
        registerLocalOperation(new SetRemove<String>(ts, e));
    }

    public boolean lookup(int e) {
        return elems.contains(e);
    }

    public Set<String> getValue() {
        return elems;
    }

}
