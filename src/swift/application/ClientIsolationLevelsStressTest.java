package swift.application;

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
 * Manual test client that stresses the system correctness by issuing mixed
 * workload of transactions with different isolation levels.
 * 
 * @author mzawirski
 */
public class ClientIsolationLevelsStressTest {
    private static final int TRANSACTIONS_PER_CLIENT = 100;
    private static final int CLIENTS_NUMBER = 10;
    static String sequencerName = "localhost";

    public static void main(String[] args) {
        // start sequencer server
        DCSequencerServer sequencer = new DCSequencerServer(sequencerName);
        sequencer.start();

        // start DC server
        DCServer.main(new String[] { sequencerName });

        Sys.init();
        final Thread[] clientThreads = new Thread[CLIENTS_NUMBER];
        for (int i = 0; i < clientThreads.length; i++) {
            final int portId = i + 2000;
            Thread clientThread = new Thread("client" + i) {
                public void run() {
                    SwiftImpl client = SwiftImpl.newInstance(portId, "localhost", DCConstants.SURROGATE_PORT);
                    runTransactions(client, portId);
                    client.stop(true);
                }
            };
            clientThreads[i] = clientThread;
            clientThread.start();
        }

        for (int i = 0; i < clientThreads.length; i++) {
            try {
                clientThreads[i].join();
            } catch (InterruptedException e) {
            }
        }
        System.out.println("clients done with processing, stopping gracefully");
        System.exit(0);
    }

    private static void runTransactions(SwiftImpl client, final int clientId) {
        try {
            for (int i = 0; i < TRANSACTIONS_PER_CLIENT; i++) {
                TxnHandle txn = client.beginTxn(i % 2 == 0 ? IsolationLevel.SNAPSHOT_ISOLATION
                        : IsolationLevel.REPEATABLE_READS, CachePolicy.STRICTLY_MOST_RECENT, false);
                IntegerTxnLocal i1 = txn.get(new CRDTIdentifier("tests", "1"), true, swift.crdt.IntegerVersioned.class,
                        TxnHandle.DUMMY_UPDATES_SUBSCRIBER);
                IntegerTxnLocal i2 = txn.get(new CRDTIdentifier("tests", "2"), true, swift.crdt.IntegerVersioned.class,
                        TxnHandle.DUMMY_UPDATES_SUBSCRIBER);

                // Play with 2 integer counters a bit.
                final int i1OldValue = i1.getValue();
                final int i2OldValue = i2.getValue();
                if (clientId % 2 == 0) {
                    i1.add(i2.getValue() - i1OldValue + 1);
                } else {
                    i2.add(i1.getValue() - i2OldValue + 1);
                }
                System.out.printf("client%d: i1.old=%d, i2.old=%d, i1.new=%d, i2.new=%d\n", clientId, i1OldValue,
                        i2OldValue, i1.getValue(), i2.getValue());
                txn.commitAsync(null);
            }
            System.out.printf("client%d done\n", clientId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
