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
 * @param <I>
 *            type of implementation for a concrete (or generic) type
 * @param <V>
 *            type of elements stored in a set
 * @see AddOnlySetCRDT
 */
public abstract class AbstractAddOnlySetCRDT<I extends AbstractAddOnlySetCRDT<I, V>, V> extends BaseCRDT<I> {
    protected Set<V> elements;

    // Kryo
    public AbstractAddOnlySetCRDT() {
    }

    public AbstractAddOnlySetCRDT(CRDTIdentifier id) {
        super(id);
        elements = new HashSet<V>();
    }

    protected AbstractAddOnlySetCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, Set<V> elements) {
        super(id, txn, clock);
        this.elements = elements;
    }

    public void add(V element) {
        elements.add(element);
        registerLocalOperation(generateAddDownstream(element));
    }

    protected AddOnlySetUpdate<I, V> generateAddDownstream(V element) {
        return new AddOnlySetUpdate<I, V>(element);
    }

    public void applyAdd(V element) {
        elements.add(element);
    }

    @Override
    public Set<V> getValue() {
        return Collections.unmodifiableSet(elements);
    }
}
