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

import static java.lang.System.exit;
import static sys.Sys.Sys;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import swift.client.SwiftOptions;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import swift.utils.SafeLog;
import swift.utils.SafeLog.ReportType;
import sys.ec2.ClosestDomain;
import sys.herd.Shepard;
import sys.scheduler.PeriodicTask;
import sys.utils.Args;
import sys.utils.IP;
import sys.utils.Progress;
import sys.utils.Props;
import sys.utils.Threading;

/**
 * Benchmark of SwiftSocial, based on data model derived from WaltSocial
 * prototype [Sovran et al. SOSP 2011].
 * <p>
 * Runs in parallel SwiftSocial sessions from the provided file. Sessions can be
 * distributed among different instances by specifying sessions range.
 */
public class SwiftSocialBenchmark extends SwiftSocialApp {

    private static String shepard;

    public void initDB(String[] args) {

        final String servers = Args.valueOf(args, "-servers", "localhost");

        String propFile = Args.valueOf(args, "-props", "swiftsocial-test.props");
        Properties properties = Props.parseFile("swiftsocial", propFile);
        SafeLog.configure(properties);

        System.err.println("Populating db with users...");

        int numUsers = Props.intValue(properties, "swiftsocial.numUsers", 1000);

        final int NumUsers = Args.valueOf(args, "-users", numUsers);
        Workload.generateUsers(NumUsers);

        int threads = Args.valueOf(args, "-threads", 6);
        final int PARTITION_SIZE = 1000;
        int partitions = numUsers / PARTITION_SIZE + (numUsers % PARTITION_SIZE > 0 ? 1 : 0);
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        final AtomicInteger counter = new AtomicInteger(0);
        for (int i = 0; i < partitions; i++) {
            int lo = i * PARTITION_SIZE, hi = (i + 1) * PARTITION_SIZE;
            final List<String> partition = Workload.getUserData().subList(lo, Math.min(hi, numUsers));
            pool.execute(new Runnable() {
                public void run() {
                    SwiftOptions options = new SwiftOptions(servers, DCConstants.SURROGATE_PORT);
                    SwiftSocialBenchmark.super.initUsers(options, partition, counter, NumUsers);
                }
            });
        }
        Threading.awaitTermination(pool, Integer.MAX_VALUE);
        Threading.sleep(5000);
        System.err.println("\nFinished populating db with users.");
    }

    public void doBenchmark(String[] args) {
        super.init(args);
        // IO.redirect("stdout.txt", "stderr.txt");
        final long startTime = System.currentTimeMillis();

        System.err.println(IP.localHostname() + "/ starting...");

        int concurrentSessions = Args.valueOf(args, "-threads", 1);
        String partitions = Args.valueOf(args, "-partition", "0/1");
        int site = Integer.valueOf(partitions.split("/")[0]);
        int numberOfSites = Integer.valueOf(partitions.split("/")[1]);
        // ASSUMPTION: concurrentSessions is the same at all sites
        int numberOfVirtualSites = numberOfSites * concurrentSessions;

        List<String> candidates = Args.subList(args, "-servers");
        server = ClosestDomain.closest2Domain(candidates, site);
        shepard = Args.valueOf(args, "-shepard", "");

        System.err.println(IP.localHostAddress() + " connecting to: " + server);

        super.populateWorkloadFromConfig();

        SafeLog.printlnComment("");
        SafeLog.printlnComment(String.format("\targs=%s", Arrays.asList(args)));
        SafeLog.printlnComment(String.format("\tsite=%s", site));
        SafeLog.printlnComment(String.format("\tnumberOfSites=%s", numberOfSites));
        SafeLog.printlnComment(String.format("\tthreads=%s", concurrentSessions));
        SafeLog.printlnComment(String.format("\tnumberOfVirtualSites=%s", numberOfVirtualSites));
        SafeLog.printlnComment(String.format("\tSurrogate=%s", server));
        SafeLog.printlnComment(String.format("\tShepard=%s", shepard));
        SafeLog.printHeader();

        if (!shepard.isEmpty())
            Shepard.sheepJoinHerd(shepard);

        // Kick off all sessions, throughput is limited by
        // concurrentSessions.
        final ExecutorService threadPool = Executors.newFixedThreadPool(concurrentSessions, Threading.factory("App"));

        System.err.println("Spawning session threads.");
        for (int i = 0; i < concurrentSessions; i++) {
            final int sessionId = site * concurrentSessions + i;
            final Workload commands = getWorkloadFromConfig(sessionId, numberOfVirtualSites);
            threadPool.execute(new Runnable() {
                public void run() {
                    // Randomize startup to avoid clients running all at the
                    // same time; causes problems akin to DDOS symptoms.
                    Threading.sleep(Sys.rg.nextInt(1000));
                    SwiftSocialBenchmark.super.runClientSession(Integer.toString(sessionId), commands, false);
                }
            });
        }

        // report client progress every 10 seconds...
        final int PERIOD = 10;
        new PeriodicTask(0.0, 0.0 + PERIOD) {
            private int lastDone;

            public void run() {
                final int recentDone = commandsDone.get();
                final int throughput = (recentDone - lastDone) / PERIOD;
                lastDone = recentDone;
                System.err.printf("Done: %s, throughput %d op/s\n",
                        Progress.percentage(recentDone, totalCommands.get()), throughput);
            }
        };

        // Wait for all sessions.
        threadPool.shutdown();
        Threading.awaitTermination(threadPool, Integer.MAX_VALUE);

        System.err.println("Session threads completed.");
        System.err.println("Throughput: " + totalCommands.get() * 1000 / (System.currentTimeMillis() - startTime)
                + " txns/s");
        System.exit(0);
    }

    public static void main(String[] args) {
        sys.Sys.init();

        SwiftSocialBenchmark instance = new SwiftSocialBenchmark();
        if (args.length == 0) {

            // DCSequencerServer.main(new String[] { "-name", "X", "-integrated"
            // });
            DCServer.main(new String[] { "-servers", "localhost", "-integrated" });

            args = new String[] { "-servers", "localhost", "-threads", "2", "-props", "swiftsocial-test.props" };
            SafeLog.configure(EnumSet.of(ReportType.APP_OP));
            instance.initDB(args);
            instance.doBenchmark(args);
            exit(0);
        }
        if (args[0].equals("-prepareDB")) {
            if (args.length != 3) {
                System.err.println(Arrays.asList(args));
                System.err.println("usage: -prepareDB dbName numUsers");
                System.exit(0);
            }
            // uses berkeleydb in sync mode to create a DB snapshot.
            // In folder db, default and default_seq need
            // to be renamed manually to 25k and 25k_seq
            DCSequencerServer.main(new String[] { "-sync", "-db", args[1], "-name", "X" });
            DCServer.main(new String[] { "-sync", "-db", args[1], "-servers", "localhost" });
            args = new String[] { "-servers", "localhost", "-users", args[2] };

            instance.initDB(args);

            Threading.sleep(60000);
            exit(0);
        }
        if (args[0].equals("-reloadDB")) {
            if (args.length != 3) {
                System.err.println(Arrays.asList(args));
                System.err.println("usage: -reloadDB dbName propfile");
                System.exit(0);
            }
            // assumes there is db/25k and db/25k_seq folders with the prepared
            // db snapshot, eg., for args[1] == 25k
            System.err.println(Arrays.asList(args));
            DCSequencerServer.main(new String[] { "-rdb", args[1], "-db", "-name", "X" });
            DCServer.main(new String[] { "-rdb", args[1], "-servers", "localhost" });
            args = new String[] { "-servers", "localhost", "-props", args[2] };

            instance.doBenchmark(args);
            exit(0);
        }
        if (args[0].equals("init")) {
            instance.initDB(args);
            exit(0);
        }
        if (args[0].equals("run")) {
            instance.doBenchmark(args);
            exit(0);
        }
    }
}

// protected static void exitWithUsage() {
// System.err.println("Usage 1: init <number_of_users>");
// System.err.println("Usage 2: run <surrogate addr> <concurrent_sessions>");
// System.exit(1);
// }

