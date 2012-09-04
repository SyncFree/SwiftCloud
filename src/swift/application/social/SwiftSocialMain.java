package swift.application.social;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import swift.client.SwiftImpl;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import swift.exceptions.SwiftException;
import sys.Sys;

/**
 * Executing SwiftSocial operations, based on data model of WaltSocial prototype
 * [Sovran et al. OSDI 2011].
 */
public class SwiftSocialMain {
    private static final long DELAY_AFTER_INIT = 3000;
    private static int lengthInputFile = 500;
    private static String dcName;
    private static String usersFileName;
    private static String commandsFileName;
    private static IsolationLevel isolationLevel;
    private static CachePolicy cachePolicy;
    private static boolean subscribeUpdates;
    private static boolean asyncCommit;

    public static void main(String[] args) {
        if (args.length != 6 && args.length != 7) {
            System.out
                    .println("Usage: <surrogate addr> <isolationLevel> <cachePolicy> <subscribe updates (true|false)>");
            System.out
                    .println("       <async. commit (true|false)> <commands filename> [users filename to initialize]");
            return;
        } else {
            dcName = args[0];
            isolationLevel = IsolationLevel.valueOf(args[1]);
            cachePolicy = CachePolicy.valueOf(args[2]);
            subscribeUpdates = Boolean.parseBoolean(args[3]);
            asyncCommit = Boolean.parseBoolean(args[4]);
            commandsFileName = args[5];
            if (args.length == 7) {
                usersFileName = args[6];
            } else {
                usersFileName = null;
            }
        }
        startSequencer();
        startDCServer();
        runClient(commandsFileName, usersFileName);
    }

    public static List<String> readInputFromFile(final String fileName) {
        List<String> data = new ArrayList<String>(lengthInputFile);
        try {
            FileInputStream fstream = new FileInputStream(fileName);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            try {
                while ((strLine = br.readLine()) != null) {
                    // read file into memory
                    data.add(strLine);
                }
            } finally {
                in.close();
            }
            return data;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void runClient(final String inputFileName, final String usersFileName) {
        Sys.init();
        Swift clientServer = SwiftImpl.newInstance(dcName, DCConstants.SURROGATE_PORT);
        SwiftSocial client = new SwiftSocial(clientServer, isolationLevel, cachePolicy, subscribeUpdates, asyncCommit);

        if (usersFileName != null) {
            initUsers(clientServer, client, usersFileName);
            clientServer.stop(true);
            try {
                Thread.sleep(DELAY_AFTER_INIT);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Execute the commands assigned to this thread
        List<String> commandData = readInputFromFile(inputFileName);
        while (true) {
            int n_fail = 0;
            for (String line : commandData) {
                String[] toks = line.split(";");
                switch (Commands.valueOf(toks[0].toUpperCase())) {
                case LOGIN:
                    if (toks.length == 3) {
                        client.login(toks[1], toks[2]);
                        break;
                    }
                case LOGOUT:
                    if (toks.length == 2) {
                        client.logout(toks[1]);
                        break;
                    }
                case READ:
                    if (toks.length == 2) {
                        client.read(toks[1], new HashSet<Message>(), new HashSet<Message>());
                        break;
                    }
                case SEE_FRIENDS:
                    if (toks.length == 2) {
                        client.readFriendList(toks[1]);
                        break;
                    }
                case FRIEND:
                    if (toks.length == 2) {
                        client.befriend(toks[1]);
                        break;
                    }
                case STATUS:
                    if (toks.length == 2) {
                        client.updateStatus(toks[1], System.currentTimeMillis());
                        break;
                    }
                case POST:
                    if (toks.length == 3) {
                        client.postMessage(toks[1], toks[2], System.currentTimeMillis());
                        break;
                    }
                default:
                    System.out.println("Can't parse command line :" + line);
                    n_fail++;
                }
            }
        }

    }

    public static void initUsers(Swift swiftClient, SwiftSocial client, final String usersFileName) {
        try {
            TxnHandle txn = swiftClient.beginTxn(IsolationLevel.REPEATABLE_READS, CachePolicy.CACHED, false);
            int txnSize = 0;
            // Initialize user data
            List<String> userData = readInputFromFile(usersFileName);
            int total = userData.size(), counter = 0;
            for (String line : userData) {
                System.out.printf("\rInitialization:%.1f%%", 100.0*counter++/total);
                // Divide into smaller transactions.
                if (txnSize >= 10000) {
                    txn.commit();
                    txn = swiftClient.beginTxn(IsolationLevel.REPEATABLE_READS, CachePolicy.CACHED, false);
                    txnSize = 0;
                } else {
                    txnSize++;
                }
                String[] toks = line.split(";");
                long birthday = 0;
                try {
                    SimpleDateFormat formatter = new SimpleDateFormat("dd/mm/yy");
                    Date dateStr = formatter.parse(toks[4]);
                    birthday = dateStr.getTime();
                } catch (ParseException e) {
                    System.err.println("Could not parse the birthdate: " + toks[4]);
                }
                client.registerUser(txn, toks[1], toks[2], toks[3], birthday, System.currentTimeMillis());
            }
            if (!txn.getStatus().isTerminated()) {
                txn.commit();
            }
        } catch (SwiftException e1) {
            e1.printStackTrace();
        }
        System.out.println("Initialization finished");
    }

    private static void startDCServer() {
        DCServer.main(new String[] { dcName });
    }

    private static void startSequencer() {
        DCSequencerServer.main(new String[] { "-name", dcName });
    }

}
