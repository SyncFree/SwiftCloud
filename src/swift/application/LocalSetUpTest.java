package swift.application;

import java.util.Random;

import swift.client.SwiftImpl;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;

/**
 * Local setup/test with one server and two clients.
 * 
 * @author annettebieniusa
 * 
 */
public class LocalSetUpTest {
    static String sequencerName = "localhost";

    public static void main(String[] args) {
        // start sequencer server
        DCSequencerServer sequencer = new DCSequencerServer(sequencerName);
        sequencer.start();

        // start DC server
        DCServer.main(new String[] { sequencerName });

        for (int i = 0; i < 5; i++) {
            final int portId = i + 2000;
            Thread client = new Thread("client" + i) {
                public void run() {
                    Sys.init();
                    SwiftImpl clientServer = SwiftImpl.newInstance(portId, "localhost", DCConstants.SURROGATE_PORT);
                    clientCode(clientServer);
                    clientServer.stop(true);
                }

            };
            client.start();
        }
    }

    private static void clientCode(SwiftImpl server) {
        try {
            TxnHandle handle = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT,
                    false);
            IntegerTxnLocal i1 = handle.get(new CRDTIdentifier("e", "1"), false, swift.crdt.IntegerVersioned.class);
            IntegerTxnLocal i2 = handle.get(new CRDTIdentifier("e", "2"), false, swift.crdt.IntegerVersioned.class);
            Random random = new Random();
            for (int i = 0; i < 5; i++) {
                i1.add(1);
                Thread.sleep(random.nextInt(500));
                i2.add(1);
            }
            handle.commit();
            System.out.println("commit");

            System.out.println(Thread.currentThread().getName() + " finished successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
