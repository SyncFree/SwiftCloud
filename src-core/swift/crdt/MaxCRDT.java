package swift.crdt;

import swift.clocks.CausalityClock;
import swift.crdt.core.BaseCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.TxnHandle;

/**
 * Maximum CRDT object.
 * 
 * @author mzawirsk
 * 
 * @param <V>
 *            a comparable type of value
 */
class MaxCRDT<V extends Comparable<V>> extends BaseCRDT<MaxCRDT<V>> {
    V max;

    public MaxCRDT() {
    }

    public MaxCRDT(CRDTIdentifier id) {
        super(id, null, null);
    }

    private MaxCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, V max) {
        super(id, txn, clock);
        this.max = max;
    }

    public void set(V value) {
        if (max == null || value.compareTo(max) > 0) {
            registerLocalOperation(new MaxUpdate<V>(value));
        }
    }

    public void applySet(V value) {
        if (max == null || value.compareTo(max) > 0) {
            max = value;
        }
    }

    /**
     * @return maximum assigned ({@link #set(Comparable)}) value, or null if
     *         there was no assignment so far
     */
    @Override
    public V getValue() {
        return max;
    }

    @Override
    public MaxCRDT<V> copy() {
        return new MaxCRDT<V>(id, txn, clock, max);
    }
}