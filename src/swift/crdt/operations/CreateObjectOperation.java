package swift.crdt.operations;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;

/**
 * Dummy operation creating object in the store.
 * 
 * @author mzawirski
 */
public class CreateObjectOperation<V extends CRDT<V>> extends BaseOperation<V> {

    // needed for kryo
    public CreateObjectOperation() {
    }
    
    public CreateObjectOperation(TripleTimestamp ts) {
        super(ts);
    }

    @Override
    public void replaceDependentOpTimestamp(Timestamp oldTs, Timestamp newTs) {
    }

    @Override
    public void applyTo(V crdt) {
        crdt.setRegisteredInStore(true);
    }
}
