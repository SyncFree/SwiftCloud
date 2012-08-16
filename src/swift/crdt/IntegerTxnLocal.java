package swift.crdt;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.IntegerUpdate;

public class IntegerTxnLocal extends BaseCRDTTxnLocal<IntegerVersioned> {
    private int val;

    public IntegerTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, IntegerVersioned creationState,
            int val) {
        super(id, txn, clock, creationState);
        this.val = val;
    }

    public Integer getValue() {
        return val;
    }

    public void add(int n) {
        val += n;
        TripleTimestamp ts = nextTimestamp();
        registerLocalOperation(new IntegerUpdate(ts, n));
    }

    public void sub(int n) {
        add(-n);
    }

    @Override
    public Object executeQuery(CRDTQuery<IntegerVersioned> query) {
        return query.executeAt(this);
    }
}
