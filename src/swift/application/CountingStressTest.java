package swift.application;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import sys.Sys;

/**
 * Manual test client that stresses the system correctness by issuing mixed
 * workload of transactions with different isolation levels.
 * 
 * @author mzawirski
 */
public class CountingStressTest {
    private static final int TRANSACTIONS_PER_CLIENT = 10;
    private static final int CLIENTS_NUMBER = 5;
    static String sequencerName = "localhost";

    public static void main(String[] args) throws NetworkException, WrongTypeException, NoSuchObjectException,
            VersionNotFoundException {
        // start sequencer server
        DCSequencerServer.main(new String[] { "-name", sequencerName });
        // DCSequencerServer sequencer = new DCSequencerServer(sequencerName);
        // sequencer.start();

        // start DC server
        DCServer.main(new String[] { sequencerName });

        Sys.init();
        final Thread[] clientThreads = new Thread[CLIENTS_NUMBER];
        for (int i = 0; i < clientThreads.length; i++) {
            final int id = i;
            Thread clientThread = new Thread("client" + i) {
                public void run() {
                    Swift client = SwiftImpl.newInstance(new SwiftOptions("localhost", DCConstants.SURROGATE_PORT));
                    runTransactions(client, id);
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

        // Check result
        Swift client = SwiftImpl.newInstance(new SwiftOptions("localhost", DCConstants.SURROGATE_PORT));
        TxnHandle txn = client.beginTxn(level, CachePolicy.STRICTLY_MOST_RECENT, false);
        IntegerTxnLocal i1 = txn.get(new CRDTIdentifier("tests", "1"), true, swift.crdt.IntegerVersioned.class,
                TxnHandle.UPDATES_SUBSCRIBER);
        assert (i1.getValue() == CLIENTS_NUMBER * TRANSACTIONS_PER_CLIENT);
        txn.commit();

        System.out.println("clients done with processing, stopping gracefully");
        System.exit(0);
    }

    static IsolationLevel level = IsolationLevel.SNAPSHOT_ISOLATION;

    private static void runTransactions(Swift client, final int clientId) {
        try {
            for (int i = 0; i < TRANSACTIONS_PER_CLIENT; i++) {
                TxnHandle txn = client.beginTxn(level, CachePolicy.STRICTLY_MOST_RECENT, false);
                IntegerTxnLocal i1 = txn.get(new CRDTIdentifier("tests", "1"), true, swift.crdt.IntegerVersioned.class,
                        TxnHandle.UPDATES_SUBSCRIBER);
                i1.add(1);

                txn.commitAsync(null);
            }
            System.out.printf("client%d done\n", clientId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
