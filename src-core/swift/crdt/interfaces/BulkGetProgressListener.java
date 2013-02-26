package swift.crdt.interfaces;

import swift.crdt.CRDTIdentifier;

public interface BulkGetProgressListener {

    void onGet(final TxnHandle txn, final CRDTIdentifier id, TxnLocalCRDT<?> view);

}
