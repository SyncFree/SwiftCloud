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
package swift.application.adservice;

import static sys.Sys.Sys;

import java.io.PrintStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import swift.exceptions.SwiftException;
import sys.utils.Args;
import sys.utils.Props;
import sys.utils.Threading;

/**
 * Executing SwiftAdService operations, based on data model of WaltSocial
 * prototype [Sovran et al. SOSP 2011].
 */
public class SwiftAdServiceMain {
    protected static String dcName;
    protected static IsolationLevel isolationLevel;
    protected static CachePolicy cachePolicy;
    protected static boolean subscribeUpdates;
    protected static boolean asyncCommit;

    protected static int thinkTime;
    protected static int adsPerCategory;
    protected static int numCategories;
    protected static int maxViewCount;
    protected static int sameCategoryFreq;
    protected static int biasedOps;
    protected static int randomOps;
    protected static int opGroups;

    protected static PrintStream bufferedOutput;

    protected static AtomicInteger commandsDone = new AtomicInteger(0);
    protected static AtomicInteger totalCommands = new AtomicInteger(0);
    private static Properties props;

    public static void main(String[] args) {
        sys.Sys.init();

        Logger.getLogger("swift").setLevel(Level.WARNING);
        // Logger.getLogger("sys").setLevel(Level.ALL);

        dcName = args.length == 0 ? "localhost" : args[0];

        init();

        SwiftOptions options = new SwiftOptions(dcName, DCConstants.SURROGATE_PORT, props);

        DCSequencerServer.main(new String[] { "-name", dcName });
        DCServer.main(new String[] { dcName });

        Props.parseFile("swiftadservice", bufferedOutput);

        System.out.println("Initializing Ads...");

        List<String> users = Workload.populate(numCategories, adsPerCategory, maxViewCount);

        initAds(options, users, new AtomicInteger(), numCategories * adsPerCategory);

        System.out.println("Waiting for 3 seconds...");

        int concurrentSessions = Args.valueOf(args, "-sessions", 1);

        Threading.sleep(3000);
        final ExecutorService sessionsExecutor = Executors.newFixedThreadPool(concurrentSessions,
                Threading.factory("Client"));

        System.err.println("Spawning session threads.");
        for (int i = 0; i < concurrentSessions; i++) {
            final int sessionId = i;
            final Workload commands = Workload.doMixed(0, adsPerCategory, biasedOps, randomOps, opGroups, 1);
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

        props = Props.parseFile("swiftadservice", bufferedOutput);
        isolationLevel = IsolationLevel.valueOf(Props.get(props, "swift.isolationLevel"));
        cachePolicy = CachePolicy.valueOf(Props.get(props, "swift.cachePolicy"));
        subscribeUpdates = Props.boolValue(props, "swift.notifications", false);
        asyncCommit = Props.boolValue(props, "swift.asyncCommit", true);

        numCategories = Props.intValue(props, "swiftadservice.numCategories", 5);
        adsPerCategory = Props.intValue(props, "swiftadservice.adsPerCategory", 100);
        maxViewCount = Props.intValue(props, "swiftadservice.maxViewCount", 1000);
        sameCategoryFreq = Props.intValue(props, "swiftadservice.sameCategoryFreq", 100);
        biasedOps = Props.intValue(props, "swiftadservice.biasedOps", 9);
        randomOps = Props.intValue(props, "swiftadservice.randomOps", 1);
        opGroups = Props.intValue(props, "swiftadservice.opGroups", 500);
        thinkTime = Props.intValue(props, "swiftadservice.thinkTime", 1000);

    }

    public static SwiftAdservice getSwiftAdService() {
        final SwiftOptions options = new SwiftOptions(dcName, DCConstants.SURROGATE_PORT, props);
        SwiftSession swiftClient = SwiftImpl.newSingleSessionInstance(options);
        SwiftAdservice adServiceClient = new SwiftAdservice(swiftClient, isolationLevel, cachePolicy, subscribeUpdates,
                asyncCommit, dcName);
        return adServiceClient;
    }

    static void runClientSession(final int sessionId, final Workload commands, boolean loop4Ever) {
        final SwiftAdservice adServiceClient = getSwiftAdService();

        totalCommands.addAndGet(commands.size());
        final long sessionStartTime = System.currentTimeMillis();
        final String initSessionLog = String.format("%d,%s,%d,%d", -1, "INIT", 0, sessionStartTime);
        bufferedOutput.println(initSessionLog);

        do
            for (String cmdLine : commands) {
                long txnStartTime = System.currentTimeMillis();
                Commands cmd = runCommandLine(adServiceClient, cmdLine);
                long txnEndTime = System.currentTimeMillis();
                final long txnExecTime = txnEndTime - txnStartTime;
                final String log = String.format("%d,%s,%d,%d", sessionId, cmd, txnExecTime, txnEndTime);
                bufferedOutput.println(log);

                Threading.sleep(thinkTime);
                commandsDone.incrementAndGet();
            }
        while (loop4Ever);

        adServiceClient.getSwift().stopScout(true);

        final long now = System.currentTimeMillis();
        final long sessionExecTime = now - sessionStartTime;
        bufferedOutput.println(String.format("%d,%s,%d,%d", sessionId, "TOTAL", sessionExecTime, now));
        bufferedOutput.flush();
    }

    public static Commands runCommandLine(SwiftAdservice adServiceClient, String cmdLine) {
        String[] toks = cmdLine.split(";");
        final Commands cmd = Commands.valueOf(toks[0].toUpperCase());
        switch (cmd) {
        case VIEW_AD:
            if (toks.length == 2) {
                adServiceClient.viewAd(toks[1]);
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

    // Adds a set of ads to the system
    public static void initAds(SwiftOptions swiftOptions, final List<String> ads, AtomicInteger counter, int total) {
        try {
            SwiftSession swiftClient = SwiftImpl.newSingleSessionInstance(swiftOptions);
            SwiftAdservice client = new SwiftAdservice(swiftClient, isolationLevel, cachePolicy, subscribeUpdates,
                    asyncCommit, swiftOptions.getServerHostname());

            TxnHandle txn = swiftClient.beginTxn(IsolationLevel.REPEATABLE_READS, CachePolicy.CACHED, false);
            int txnSize = 0;
            // Initialize user data
            List<String> adsData = ads;
            for (String line : adsData) {

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
                client.addAd(txn, toks[1], toks[3], Integer.parseInt(toks[5]));
            }
            if (!txn.getStatus().isTerminated()) {
                txn.commit();
            }
            swiftClient.stopScout(true);
        } catch (SwiftException e1) {
            e1.printStackTrace();
        }
    }
}
