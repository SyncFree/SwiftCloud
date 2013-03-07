/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.application.social;

import static sys.Sys.Sys;

import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.SwiftSession;
import swift.crdt.interfaces.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import swift.exceptions.SwiftException;
import sys.utils.Args;
import sys.utils.Props;
import sys.utils.Threading;

/**
 * Executing SwiftSocial operations, based on data model of WaltSocial prototype
 * [Sovran et al. SOSP 2011].
 */
public class SwiftSocialMain {
    protected static String dcName;
    protected static IsolationLevel isolationLevel;
    protected static CachePolicy cachePolicy;
    protected static boolean subscribeUpdates;
    protected static boolean asyncCommit;
    protected static int asyncQueueSize;
    protected static int batchSize;
    protected static int cacheSize;
    protected static int cacheEvictionTimeMillis;

    protected static int thinkTime;
    protected static int numUsers;
    protected static int userFriends;
    protected static int biasedOps;
    protected static int randomOps;
    protected static int opGroups;

    protected static PrintStream bufferedOutput;

    protected static AtomicInteger commandsDone = new AtomicInteger(0);
    protected static AtomicInteger totalCommands = new AtomicInteger(0);

    public static void main(String[] args) {
        sys.Sys.init();

        Logger.getLogger("swift").setLevel(Level.WARNING);
        // Logger.getLogger("sys").setLevel(Level.ALL);

        dcName = args.length == 0 ? "localhost" : args[0];

        init();

        SwiftOptions options = new SwiftOptions(dcName, DCConstants.SURROGATE_PORT);
        options.setCacheEvictionTimeMillis(cacheEvictionTimeMillis);
        options.setCacheSize(cacheSize);
        options.setMaxAsyncTransactionsQueued(asyncQueueSize);
        options.setMaxCommitBatchSize(batchSize);

        startSequencer();
        startDCServer();

        Props.parseFile("swiftsocial", bufferedOutput);

        System.out.println("Initializing Users...");

        initUsers(options, Workload.populate(numUsers), new AtomicInteger(), numUsers);

        System.out.println("Waiting for 3 seconds...");

        int concurrentSessions = Args.valueOf(args, "-sessions", 2);

        Threading.sleep(3000);
        final ExecutorService sessionsExecutor = Executors.newFixedThreadPool(concurrentSessions,
                Threading.factory("Client"));

        System.err.println("Spawning session threads.");
        for (int i = 0; i < concurrentSessions; i++) {
            final int sessionId = i;
            final Workload commands = Workload.doMixed(0, userFriends, biasedOps, randomOps, opGroups, 1);
            sessionsExecutor.execute(new Runnable() {
                public void run() {
                    // Randomize startup to avoid clients running all at the
                    // same time; causes problems akin to DDOS symptoms.
                    Threading.sleep(Sys.rg.nextInt(1000));
                    runClientSession(sessionId, commands, false);
                }
            });
        }

    }

    public static void init() {

        bufferedOutput = new PrintStream(System.out, false);

        Props.parseFile("swiftsocial", bufferedOutput);
        isolationLevel = IsolationLevel.valueOf(Props.get("swift.IsolationLevel"));
        cachePolicy = CachePolicy.valueOf(Props.get("swift.CachePolicy"));
        subscribeUpdates = Props.boolValue("swift.Notifications", false);
        asyncCommit = Props.boolValue("swift.AsyncCommit", true);
        asyncQueueSize = Props.intValue("swift.AsyncQueue", 50);
        cacheEvictionTimeMillis = Props.intValue("swift.cacheEvictionTimeMillis", 120000);
        cacheSize = Props.intValue("swift.CacheSize", 1024);
        batchSize = Props.intValue("swift.BatchSize", 10);

        numUsers = Props.intValue("swiftsocial.numUsers", 1000);
        userFriends = Props.intValue("swiftsocial.userFriends", 25);
        biasedOps = Props.intValue("swiftsocial.biasedOps", 9);
        randomOps = Props.intValue("swiftsocial.biasedOps", 1);
        opGroups = Props.intValue("swiftsocial.opGroups", 500);
        thinkTime = Props.intValue("swiftsocial.thinkTime", 1000);

    }

    public static SwiftSocial getSwiftSocial() {
        final SwiftOptions options = new SwiftOptions(dcName, DCConstants.SURROGATE_PORT);
        options.setCacheEvictionTimeMillis(cacheEvictionTimeMillis);
        options.setCacheSize(cacheSize);
        options.setMaxAsyncTransactionsQueued(asyncQueueSize);
        options.setMaxCommitBatchSize(batchSize);
        options.setDisasterSafe(false);
        SwiftSession swiftClient = SwiftImpl.newSingleSessionInstance(options);
        SwiftSocial socialClient = new SwiftSocial(swiftClient, isolationLevel, cachePolicy, subscribeUpdates,
                asyncCommit);
        return socialClient;
    }

    static void runClientSession(final int sessionId, final Workload commands, boolean loop4Ever) {
        final SwiftSocial socialClient = getSwiftSocial();

        totalCommands.addAndGet(commands.size());
        final long sessionStartTime = System.currentTimeMillis();
        final String initSessionLog = String.format("%d,%s,%d,%d", -1, "INIT", 0, sessionStartTime);
        bufferedOutput.println(initSessionLog);

        do
            for (String cmdLine : commands) {
                long txnStartTime = System.currentTimeMillis();
                Commands cmd = runCommandLine(socialClient, cmdLine);
                long txnEndTime = System.currentTimeMillis();
                final long txnExecTime = txnEndTime - txnStartTime;
                final String log = String.format("%d,%s,%d,%d", sessionId, cmd, txnExecTime, txnEndTime);
                bufferedOutput.println(log);

                Threading.sleep(thinkTime);
                commandsDone.incrementAndGet();
            }
        while (loop4Ever);

        socialClient.getSwift().stopScout(true);

        final long now = System.currentTimeMillis();
        final long sessionExecTime = now - sessionStartTime;
        bufferedOutput.println(String.format("%d,%s,%d,%d", sessionId, "TOTAL", sessionExecTime, now));
        bufferedOutput.flush();
    }

    public static Commands runCommandLine(SwiftSocial socialClient, String cmdLine) {
        String[] toks = cmdLine.split(";");
        final Commands cmd = Commands.valueOf(toks[0].toUpperCase());
        switch (cmd) {
        case LOGIN:
            if (toks.length == 3) {
                while (!socialClient.login(toks[1], toks[2]))
                    Threading.sleep(1000);
                break;
            }
        case LOGOUT:
            if (toks.length == 2) {
                socialClient.logout(toks[1]);
                break;
            }
        case READ:
            if (toks.length == 2) {
                socialClient.read(toks[1], new HashSet<Message>(), new HashSet<Message>());
                break;
            }
        case SEE_FRIENDS:
            if (toks.length == 2) {
                socialClient.readFriendList(toks[1]);
                break;
            }
        case FRIEND:
            if (toks.length == 2) {
                socialClient.befriend(toks[1]);
                break;
            }
        case STATUS:
            if (toks.length == 2) {
                socialClient.updateStatus(toks[1], System.currentTimeMillis());
                break;
            }
        case POST:
            if (toks.length == 3) {
                socialClient.postMessage(toks[1], toks[2], System.currentTimeMillis());
                break;
            }
        default:
            System.err.println("Can't parse command line :" + cmdLine);
            System.err.println("Exiting...");
            System.exit(1);
        }
        return cmd;
    }

    static String progressMsg = "";

    public static void initUsers(SwiftOptions swiftOptions, final List<String> users, AtomicInteger counter, int total) {
        try {
            SwiftSession swiftClient = SwiftImpl.newSingleSessionInstance(swiftOptions);
            SwiftSocial client = new SwiftSocial(swiftClient, isolationLevel, cachePolicy, subscribeUpdates,
                    asyncCommit);

            TxnHandle txn = swiftClient.beginTxn(IsolationLevel.REPEATABLE_READS, CachePolicy.CACHED, false);
            int txnSize = 0;
            // Initialize user data
            List<String> userData = users;
            for (String line : userData) {

                String msg = String.format("Initialization:%.0f%%", 100.0 * counter.incrementAndGet() / total);
                if (!msg.equals(progressMsg)) {
                    progressMsg = msg;
                    System.out.println(progressMsg);
                }
                // Divide into smaller transactions.
                if (txnSize >= 100) {
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
            swiftClient.stopScout(true);
        } catch (SwiftException e1) {
            e1.printStackTrace();
        }
    }

    private static void startDCServer() {
        DCServer.main(new String[] { dcName });
    }

    private static void startSequencer() {
        DCSequencerServer.main(new String[] { "-name", dcName });
    }

}
