package swift.crdt;

import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTUpdate;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

@SuppressWarnings("serial")
public class DirectoryVersioned extends BaseCRDT<DirectoryVersioned> {

    @Override
    public void rollback(Timestamp ts) {
        throw new RuntimeException("Not implemented yet!");
    }

    @Override
    protected void pruneImpl(CausalityClock pruningPoint) {
        throw new RuntimeException("Not implemented yet!");
    }

    @Override
    protected void mergePayload(DirectoryVersioned otherObject) {
        throw new RuntimeException("Not implemented yet!");
    }

    protected void execute(CRDTUpdate<DirectoryVersioned> op) {
        op.applyTo(this);
    }

    public void applyPut(String key, CRDT<?> val, Set<Timestamp> toBeRemoved, TripleTimestamp tripleTimestamp) {
        throw new RuntimeException("Not implemented yet!");
    }

    public void applyRemove(String key, Set<Timestamp> toBeRemoved, TripleTimestamp tripleTimestamp) {
        throw new RuntimeException("Not implemented yet!");
    }

    @Override
    protected TxnLocalCRDT<DirectoryVersioned> getTxnLocalCopyImpl(CausalityClock versionClock, TxnHandle txn) {
        throw new RuntimeException("Not implemented yet!");
    }

    @Override
    protected Set<Timestamp> getUpdateTimestampsSinceImpl(CausalityClock clock) {
        throw new RuntimeException("Not implemented yet!");
    }

}
