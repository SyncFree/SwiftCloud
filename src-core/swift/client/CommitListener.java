package swift.client;

import swift.crdt.interfaces.TxnHandle;

/**
 * Notification mechanism for asynchronous transaction commit.
 * 
 * @author mzawirski
 */
public interface CommitListener {
    /**
     * Specifies action called on global commit of a transaction.
     * 
     * @param transaction
     *            globally committed transaction
     */
    void onGlobalCommit(TxnHandle transaction);
}
