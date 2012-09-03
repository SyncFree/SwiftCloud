package swift.crdt.operations;

import java.util.Set;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.DirectoryVersioned;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTUpdate;

public class DirectoryPutUpdate extends BaseUpdate<DirectoryVersioned> {
    CRDT<?> val;
    Set<TripleTimestamp> toBeRemoved;
    String key;

    public DirectoryPutUpdate(String key, CRDT<?> val, Set<TripleTimestamp> toBeRemoved, TripleTimestamp ts) {
        // TODO Check that key and type of CRDT are consistent
        super(ts);
        this.key = key;
        this.val = val;
        this.toBeRemoved = toBeRemoved;
    }

    @Override
    public CRDTUpdate<DirectoryVersioned> withBaseTimestamp(Timestamp ts) {
        throw new RuntimeException("Not implemented yet!");

    }

    @Override
    public void replaceDependeeOperationTimestamp(Timestamp oldTs, Timestamp newTs) {
        throw new RuntimeException("Not implemented yet!");

    }

    @Override
    public void applyTo(DirectoryVersioned crdt) {
        crdt.applyPut(key, val, toBeRemoved, getTimestamp());
    }

}
