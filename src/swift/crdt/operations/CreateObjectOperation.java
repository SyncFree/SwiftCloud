package swift.crdt.operations;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;

/**
 * Dummy operation creating object in the store.
 * 
 * @author mzawirski
 */
public class CreateObjectOperation<V extends CRDT<V>> extends BaseOperation<V> {
    protected V creationState;

    // needed for kryo
    public CreateObjectOperation() {
    }

    public CreateObjectOperation(TripleTimestamp ts, final V creationState) {
        super(ts);
        this.creationState = creationState;
    }

    @Override
    public void replaceDependentOpTimestamp(Timestamp oldTs, Timestamp newTs) {
    }

    @Override
    public void applyTo(V crdt) {
        crdt.setRegisteredInStore(true);
    }

    @Override
    public boolean hasCreationState() {
        return true;
    }

    @Override
    public V getCreationState() {
        return creationState;
    }

    @Override
    public CRDTOperation<V> withBaseTimestamp(Timestamp ts) {
        return new CreateObjectOperation<V>(getTimestamp().withBaseTimestamp(ts), creationState);
    }
}
