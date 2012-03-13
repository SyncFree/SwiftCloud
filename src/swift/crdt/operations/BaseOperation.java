package swift.crdt.operations;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTOperation;

public abstract class BaseOperation implements CRDTOperation {
    private TripleTimestamp ts;

    protected BaseOperation(TripleTimestamp ts) {
        this.ts = ts;
    }

    @Override
    public TripleTimestamp getTimestamp() {
        return this.ts;
    }

    @Override
    public void replaceBaseTimestamp(Timestamp newBaseTimestamp) {
        ts = ts.withBaseTimestamp(newBaseTimestamp);
    }
}
