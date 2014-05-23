package swift.crdt;

import swift.crdt.core.CRDTUpdate;

public class AddOnlySetUpdate<V> implements CRDTUpdate<AddOnlySetCRDT<V>> {
    private V element;

    // Kryo
    public AddOnlySetUpdate() {
    }

    public AddOnlySetUpdate(V element) {
        this.element = element;
    }

    @Override
    public void applyTo(AddOnlySetCRDT<V> crdt) {
        crdt.applyAdd(element);
    }

    @Override
    public Object getValueWithoutMetadata() {
        return element;
    }
}
