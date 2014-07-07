package notifications;

import java.util.Properties;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.client.SwiftImpl.CacheUpdateProtocol;
import swift.crdt.IntegerCRDT;
import swift.crdt.core.CRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.ObjectUpdatesListener;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;

public class Test2 {

    public static void main(String[] args) throws Exception {

        DCSequencerServer.main(new String[] { "-name", "X" });
        DCServer.main(new String[] { "-servers", "localhost" });

        final CRDTIdentifier id = new CRDTIdentifier("/integers", "1");

        final SwiftOptions options = new SwiftOptions("localhost", DCConstants.SURROGATE_PORT, new Properties());
        options.getCacheUpdateProtocol();
        options.setCacheUpdateProtocol(CacheUpdateProtocol.CAUSAL_NOTIFICATIONS_STREAM);
        options.setCacheSize(100);
        options.setDisasterSafe(true);

        SwiftSession server = SwiftImpl.newSingleSessionInstance(options);

        for (;;) {
            IntegerCRDT i = null;
            TxnHandle txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, false);
            try {
                i = (IntegerCRDT) txn.get(id, false, IntegerCRDT.class, new ObjectUpdatesListener() {

                    @Override
                    public void onObjectUpdate(TxnHandle txn, CRDTIdentifier id, CRDT<?> previousValue) {
                        System.err.println(id + "/>>>" + previousValue.getValue());
                    }

                    @Override
                    public boolean isSubscriptionOnly() {
                        return false;
                    }
                });
                System.err.println(i.getValue() + "/" + i.getClock());
            } catch (Exception x) {
                x.printStackTrace();
            }
            txn.commit();
            Thread.sleep(1000);
        }
    }
}
