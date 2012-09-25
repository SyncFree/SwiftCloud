package swift.crdt.operations;

import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.DirectoryVersioned;

public class DirectoryCreateUpdate extends BaseUpdate<DirectoryVersioned> {
    CRDTIdentifier entry;

    public DirectoryCreateUpdate() {
        // method stub for kryo...
    }

    public DirectoryCreateUpdate(CRDTIdentifier val, TripleTimestamp ts) {
        // TODO Check that key and type of CRDT are consistent
        super(ts);
        this.entry = val;
    }

    @Override
    public void applyTo(DirectoryVersioned crdt) {
        crdt.applyPut(entry, getTimestamp());
    }

}
