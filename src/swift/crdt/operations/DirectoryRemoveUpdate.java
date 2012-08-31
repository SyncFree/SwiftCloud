package swift.crdt.operations;

import java.util.Set;

import swift.clocks.Timestamp;
import swift.crdt.DirectoryVersioned;
import swift.crdt.interfaces.CRDTUpdate;

public class DirectoryRemoveUpdate extends BaseUpdate<DirectoryVersioned> {
    private Set<Timestamp> ids;
    private String key;

    public DirectoryRemoveUpdate() {
        // Method stub for kryo
    }

    public DirectoryRemoveUpdate(String key, Set<Timestamp> ids) {
        this.key = key;
        this.ids = ids;
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
        throw new RuntimeException("Not implemented yet!");

    }

}
