package notifications;

import java.util.Properties;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.IntegerCRDT;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;

public class Test1 {

    public static void main(String[] args) throws Exception {

        DCSequencerServer.main(new String[] { "-name", "X" });
        DCServer.main(new String[] { "-servers", "localhost" });

        final CRDTIdentifier id = new CRDTIdentifier("/integers", "1");

        final SwiftOptions options = new SwiftOptions("localhost", DCConstants.SURROGATE_PORT, new Properties());
        options.assumeAtomicCausalNotifications();
        options.setCausalNotifications(true);
        options.setCacheSize(100);
        options.setDisasterSafe(true);

        SwiftSession server = SwiftImpl.newSingleSessionInstance(options);

        for (;;) {
            TxnHandle txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, false);
            IntegerCRDT i = (IntegerCRDT) txn.get(id, true, IntegerCRDT.class);
            i.add(1);
            System.err.println("---->" + i.getValue());
            txn.commit();
            Thread.sleep(1000);
        }
    }
}
