package swift.crdt.operations;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDTOperation;

public abstract class BaseOperation implements CRDTOperation {
    private CRDTIdentifier target;
    private TripleTimestamp ts;
    private CausalityClock c;

    protected BaseOperation(CRDTIdentifier target, TripleTimestamp ts, CausalityClock c) {
        this.target = target;
        this.ts = ts;
        this.c = c;
    }

    @Override
    public CRDTIdentifier getTargetUID() {
        return this.target;
    }

    @Override
    public TripleTimestamp getTimestamp() {
        return this.ts;
    }

    @Override
    public void replaceBaseTimestamp(Timestamp newBaseTimestamp) {
        ts = ts.withBaseTimestamp(newBaseTimestamp);
    }

    @Override
    public CausalityClock getDependency() {
        return this.c;
    }

}
