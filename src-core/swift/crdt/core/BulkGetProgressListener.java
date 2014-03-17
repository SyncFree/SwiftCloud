package swift.crdt.core;


public interface BulkGetProgressListener {

    void onGet(final TxnHandle txn, final CRDTIdentifier id, CRDT<?> view);

}
