package swift.crdt;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

/**
 * @author mzawirsk
 * @param <V>
 */
public class AddOnlySetCRDT<V> extends BaseCRDT<AddOnlySetCRDT<V>> {
    private Set<V> elements;

    // Kryo
    public AddOnlySetCRDT() {
    }

    public AddOnlySetCRDT(CRDTIdentifier id) {
        super(id);
        elements = new HashSet<V>();
    }

    private AddOnlySetCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, Set<V> elements) {
        super(id, txn, clock);
        this.elements = elements;
    }

    public void add(V element) {
        elements.add(element);
        registerLocalOperation(new AddOnlySetUpdate<V>(element));
    }

    public void applyAdd(V element) {
        elements.add(element);
    }

    @Override
    public Set<V> getValue() {
        return Collections.unmodifiableSet(elements);
    }

    @Override
    public AddOnlySetCRDT<V> copy() {
        return new AddOnlySetCRDT<V>(id, txn, clock, new HashSet<V>(elements));
    }

}
