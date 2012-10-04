package swift.crdt.operations;

import java.util.HashSet;
import java.util.Set;

import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.DirectoryVersioned;

public class DirectoryRemoveUpdate extends BaseUpdate<DirectoryVersioned> {
    private Set<TripleTimestamp> toBeRemoved;
    private CRDTIdentifier key;

    public DirectoryRemoveUpdate() {
        // Method stub for kryo
    }

    public DirectoryRemoveUpdate(CRDTIdentifier key, Set<TripleTimestamp> toBeRemoved, TripleTimestamp ts) {
        super(ts);
        this.key = key;
        this.toBeRemoved = new HashSet<TripleTimestamp>();
        for (final TripleTimestamp toBeRemovedTs : toBeRemoved) {
            this.toBeRemoved.add(toBeRemovedTs.copyWithCleanedMappings());
        }
    }

    @Override
    public void applyTo(DirectoryVersioned crdt) {
        crdt.applyRemove(key, toBeRemoved, getTimestamp());
    }

}
