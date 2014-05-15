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

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import swift.client.SwiftOptions;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
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

        String servers = Args.valueOf(args, "-servers", "localhost");

        Properties properties = Props.parseFile("swiftsocial", System.out, "swiftsocial-test.props");

        final SwiftOptions options = new SwiftOptions(servers, DCConstants.SURROGATE_PORT);

        options.setConcurrentOpenTransactions(true);

        System.out.println("Populating db with users...");

        final int numUsers = Props.intValue(properties, "swiftsocial.numUsers", 1000);
        Workload.generateUsers(numUsers);

        final int PARTITION_SIZE = 1000;
        int partitions = numUsers / PARTITION_SIZE + (numUsers % PARTITION_SIZE > 0 ? 1 : 0);
        ExecutorService pool = Executors.newFixedThreadPool(4);

        final AtomicInteger counter = new AtomicInteger(0);
        for (int i = 0; i < partitions; i++) {
            int lo = i * PARTITION_SIZE, hi = (i + 1) * PARTITION_SIZE;
            final List<String> partition = Workload.getUserData().subList(lo, Math.min(hi, numUsers));
            pool.execute(new Runnable() {
                public void run() {
                    SwiftSocialBenchmark.super.initUsers(options, partition, counter, numUsers);
                }
            });
        }
        Threading.awaitTermination(pool, Integer.MAX_VALUE);
        Threading.sleep(5000);
        System.out.println("\nFinished populating db with users.");
    }

    public void doBenchmark(String[] args) {
        // IO.redirect("stdout.txt", "stderr.txt");

        System.err.println(IP.localHostname() + "/ starting...");

        int concurrentSessions = Args.valueOf(args, "-threads", 1);
        String partitions = Args.valueOf(args, "-partition", "0/1");
        int site = Integer.valueOf(partitions.split("/")[0]);
        int numberOfSites = Integer.valueOf(partitions.split("/")[1]);

        List<String> candidates = Args.subList(args, "-servers");
        server = ClosestDomain.closest2Domain(candidates, site);
        shepard = Args.valueOf(args, "-shepard", "");

        System.err.println(IP.localHostAddress() + " connecting to: " + server);

        bufferedOutput = new PrintStream(System.out, false);

        super.populateWorkloadFromConfig();

        bufferedOutput.printf(";\n;\targs=%s\n", Arrays.asList(args));
        bufferedOutput.printf(";\tsite=%s\n", site);
        bufferedOutput.printf(";\tnumberOfSites=%s\n", numberOfSites);
        bufferedOutput.printf(";\tSurrogate=%s\n", server);
        bufferedOutput.printf(";\tShepard=%s\n", shepard);
        bufferedOutput.printf(";\tthreads=%s\n;\n", concurrentSessions);

        if (!shepard.isEmpty())
            Shepard.sheepJoinHerd(shepard);

        // Kick off all sessions, throughput is limited by
        // concurrentSessions.
        final ExecutorService threadPool = Executors.newFixedThreadPool(concurrentSessions, Threading.factory("App"));

        System.err.println("Spawning session threads.");
        for (int i = 0; i < concurrentSessions; i++) {
            final int sessionId = i;
            final Workload commands = getWorkloadFromConfig(site, numberOfSites);
            threadPool.execute(new Runnable() {
                public void run() {
                    // Randomize startup to avoid clients running all at the
                    // same time; causes problems akin to DDOS symptoms.
                    Threading.sleep(Sys.rg.nextInt(1000));
                    SwiftSocialBenchmark.super.runClientSession(Integer.toString(sessionId), commands, false);
                }
            });
        }

        // report client progress every 1 seconds...
        new PeriodicTask(0.0, 1.0) {
            public void run() {
                System.err.printf("\rDone: %s", Progress.percentage(commandsDone.get(), totalCommands.get()));
            }
        };

        // Wait for all sessions.
        threadPool.shutdown();
        Threading.awaitTermination(threadPool, Integer.MAX_VALUE);

        System.err.println("Session threads completed.");
        System.exit(0);
    }

    public static void main(String[] args) {
        sys.Sys.init();

        SwiftSocialBenchmark instance = new SwiftSocialBenchmark();
        if (args.length == 0) {

            DCSequencerServer.main(new String[] { "-name", "X0" });
            DCServer.main(new String[] { "-servers", "localhost" });

            args = new String[] { "-servers", "localhost", "-threads", "3" };

            instance.initDB(args);
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

