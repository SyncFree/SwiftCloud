package swift.crdt.interfaces;

public interface CRDTQuery<V extends CRDT<V>> {
    Object executeAt(TxnLocalCRDT<V> crdtVersion);
}
