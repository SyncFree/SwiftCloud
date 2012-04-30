package swift.application;

import swift.client.SwiftImpl;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerTxnLocal;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.TxnHandle;
import swift.dc.DCConstants;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.utils.NanoTimeCollector;
import sys.Sys;

/**
 * Local setup/test with one server and two clients.
 * 
 * @author annettebieniusa
 * 
 */
public class PingSpeedBenchmark {
    private static String sequencerName;
    private static String dcName;
    private static int iterations;
    private static int clientId;
    private static int portId;

    public static void main(String[] args) {
        if (args.length != 6) {
            System.out.println("[number of iterations] [client id (1|2)] [sequencer] [port]");
            return;
        } else {
            iterations = Integer.parseInt(args[1]);
            clientId = Integer.parseInt(args[2]);
            sequencerName = args[3];
            portId = Integer.parseInt(args[4]);
        }

        Sys.init();
        SwiftImpl clientServer = SwiftImpl.newInstance(portId, dcName, DCConstants.SURROGATE_PORT);

        if (clientId == 1) {
            client1Code(clientServer);
        } else if (clientId == 2) {
            client2Code(clientServer);
        }
        clientServer.stop(true);
    }

    private static void client1Code(SwiftImpl server) {
        try {
            NanoTimeCollector timer = new NanoTimeCollector();
            timer.start();
            TxnHandle handle = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT,
                    false);
            IntegerTxnLocal i1 = handle.get(new CRDTIdentifier("e", "1"), true, swift.crdt.IntegerVersioned.class);
            i1.add(1);
            handle.commit();
            int expected = 2;

            while (true) {
                TxnHandle txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT,
                        false);
                IntegerTxnLocal i = txn.get(new CRDTIdentifier("e", "1"), false, swift.crdt.IntegerVersioned.class);
                if (expected == i.getValue()) {
                    long pingTime = timer.stop();
                    txn.commit();
                    System.out.println("Ping time: " + pingTime);

                    if (expected / 2 < iterations) {
                        // wait for the system to settle down and finish
                        // internals
                        Thread.sleep(1000);
                        expected += 2;
                        timer.start();
                        increment(server);
                    } else {
                        break;
                    }
                } else {
                    // System.out.println("Value " + i.getValue());
                    txn.rollback();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void increment(SwiftImpl server) throws NetworkException, WrongTypeException, NoSuchObjectException,
            VersionNotFoundException {
        TxnHandle handle = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
        IntegerTxnLocal i1 = handle.get(new CRDTIdentifier("e", "1"), false, swift.crdt.IntegerVersioned.class);
        i1.add(1);
        handle.commit();
    }

    private static void client2Code(SwiftImpl server) {
        try {
            int expected = 1;
            while (true) {
                TxnHandle handle = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT,
                        false);
                IntegerTxnLocal i1 = handle.get(new CRDTIdentifier("e", "1"), false, swift.crdt.IntegerVersioned.class);
                if (i1.getValue() == expected) {
                    i1.add(1);
                    handle.commit();
                    expected += 2;
                } else {
                    handle.rollback();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
