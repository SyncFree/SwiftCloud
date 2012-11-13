package swift.client;

import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.ObjectUpdatesListener;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;

/**
 * Base class for {@link ObjectUpdatesListener} implementations expecting
 * notifications.
 * 
 * @author mzawirski
 */
public abstract class AbstractObjectUpdatesListener implements ObjectUpdatesListener {
    @Override
    public abstract void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, TxnLocalCRDT<?> previousValue);

    @Override
    public boolean isSubscriptionOnly() {
        return false;
    }
}
