package swift.crdt;

import swift.crdt.core.CRDTUpdate;

public class AddOnlySetUpdate<I extends AbstractAddOnlySetCRDT<I, V>, V> implements CRDTUpdate<I> {
    protected V element;

    // Kryo
    public AddOnlySetUpdate() {
    }

    public AddOnlySetUpdate(V element) {
        this.element = element;
    }

    @Override
    public void applyTo(I crdt) {
        crdt.applyAdd(element);
    }

    @Override
    public Object getValueWithoutMetadata() {
        return element;
    }
}
