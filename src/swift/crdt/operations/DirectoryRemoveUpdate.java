package swift.crdt.operations;

import java.util.Set;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.DirectoryVersioned;
import swift.crdt.interfaces.CRDTUpdate;

public class DirectoryRemoveUpdate extends BaseUpdate<DirectoryVersioned> {
    private Set<TripleTimestamp> toBeRemoved;
    private CRDTIdentifier key;

    public DirectoryRemoveUpdate() {
        // Method stub for kryo
    }

    public DirectoryRemoveUpdate(CRDTIdentifier key, Set<TripleTimestamp> toBeRemoved, TripleTimestamp ts) {
        super(ts);
        this.key = key;
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
        crdt.applyRemove(key, toBeRemoved, getTimestamp());
    }

}
