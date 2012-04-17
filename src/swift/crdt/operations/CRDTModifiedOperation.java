package swift.crdt.operations;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;

/**
 * Class used to notify that a CRDT has been modified.
 * @author nmp
 *
 */
public class CRDTModifiedOperation <V extends CRDT<V>> implements CRDTOperation<V> {

    @Override
    public TripleTimestamp getTimestamp() {
        return null;
    }

    @Override
    public CRDTOperation<V> withBaseTimestamp(Timestamp ts) {
        return null;
    }

    @Override
    public void replaceDependentOpTimestamp(Timestamp oldTs, Timestamp newTs) {
        // do nothing
    }

    @Override
    public void applyTo(V crdt) {
        // do nothing
        
    }

}
