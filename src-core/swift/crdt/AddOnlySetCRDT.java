package swift.crdt;

import java.util.HashSet;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

/**
 * A generic add-only set.
 * 
 * @author mzawirsk
 * @param <V>
 *            elements type
 */
public class AddOnlySetCRDT<V> extends AbstractAddOnlySetCRDT<AddOnlySetCRDT<V>, V> {
    // Kryo
    public AddOnlySetCRDT() {
    }

    public AddOnlySetCRDT(CRDTIdentifier id) {
        super(id);
    }

    private AddOnlySetCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, Set<V> elements) {
        super(id, txn, clock, elements);
    }

    @Override
    public AddOnlySetCRDT<V> copy() {
        return new AddOnlySetCRDT<V>(id, txn, clock, new HashSet<V>(elements));
    }

}
