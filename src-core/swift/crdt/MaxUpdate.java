package swift.crdt;

import swift.crdt.core.CRDTUpdate;

public class MaxUpdate<V extends Comparable<V>> implements CRDTUpdate<MaxCRDT<V>> {
    V value;

    public MaxUpdate(V value) {
        this.value = value;
    }

    @Override
    public void applyTo(MaxCRDT<V> crdt) {
        crdt.applySet(value);
    }

    @Override
    public Object getValueWithoutMetadata() {
        return value;
    }
}