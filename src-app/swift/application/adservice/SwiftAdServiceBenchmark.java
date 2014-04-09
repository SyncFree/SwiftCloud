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

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import swift.client.SwiftOptions;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.ec2.ClosestDomain;
import sys.scheduler.PeriodicTask;
import sys.shepard.Shepard;
import sys.utils.Args;
import sys.utils.IP;
import sys.utils.Props;
import sys.utils.Threading;

/**
 * Benchmark of SwiftAdSerive, based on data model derived from WaltSocial
 * prototype [Sovran et al. SOSP 2011].
 * <p>
 * Runs in parallel SwiftAdSerive sessions from the provided file. Sessions can
 * be distributed among different instances by specifying sessions range.
 */
public class SwiftAdServiceBenchmark extends SwiftAdServiceMain {
    private static String shepard;

    public static void main(String[] args) {

        final String command = args[0];
        sys.Sys.init();

        if (command.equals("init")) {

            dcName = Args.valueOf(args, "-servers", "localhost");
            if (dcName.equals("@")) {
                dcName = "localhost";
                DCServer.main(new String[] { dcName });
                DCSequencerServer.main(new String[] { "-name", "INIT" });
            }

            Properties properties = Props.parseFile("swiftadservice", System.out);
            final SwiftOptions options = new SwiftOptions(dcName, DCConstants.SURROGATE_PORT);
            options.setConcurrentOpenTransactions(true);

            System.out.println("Populating db with advertisements...");

            final int adsPerCategory = Props.intValue(properties, "swiftadservice.adsPerCateggory", 5);
            final int numCategories = Props.intValue(properties, "swiftadservice.numCategories", 100);
            final int maxViewCount = Props.intValue(properties, "swiftadservice.maxViewCount", 1000);
            List<String> ads = Workload.populate(numCategories, adsPerCategory, maxViewCount);

            final int PARTITION_SIZE = 1000;
            int partitions = ads.size() / PARTITION_SIZE + (ads.size() % PARTITION_SIZE > 0 ? 1 : 0);
            ExecutorService pool = Executors.newFixedThreadPool(4);

            final AtomicInteger counter = new AtomicInteger(0);
            for (int i = 0; i < partitions; i++) {
                int lo = i * PARTITION_SIZE, hi = (i + 1) * PARTITION_SIZE;
                final List<String> partition = ads.subList(lo, Math.min(hi, ads.size()));
                pool.execute(new Runnable() {
                    public void run() {
                        SwiftAdServiceMain.initAds(options, partition, counter, numCategories * adsPerCategory);
                    }
                });
            }
            // Wait for all sessions.
            pool.shutdown();
            try {
                pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Threading.sleep(5000);
            System.out.println("\nFinished populating db with ads.");
        }
        if (command.equals("run")) {

            // IO.redirect("stdout.txt", "stderr.txt");
            System.err.println(IP.localHostname() + "/ starting...");

            int concurrentSessions = Args.valueOf(args, "-threads", 1);
            String partitions = Args.valueOf(args, "-partition", "0/1");
            int site = Integer.valueOf(partitions.split("/")[0]);
            int numberOfSites = Integer.valueOf(partitions.split("/")[1]);

            List<String> servers = Args.subList(args, "-servers");
            dcName = ClosestDomain.closest2Domain(servers, site);
            shepard = Args.valueOf(args, "-shepard", dcName);

            System.err.println(IP.localHostAddress() + " connecting to: " + dcName);

            SwiftAdServiceMain.init();

            bufferedOutput.printf(";\n;\targs=%s\n", Arrays.asList(args));
            bufferedOutput.printf(";\tsite=%s\n", site);
            bufferedOutput.printf(";\tnumberOfSites=%s\n", numberOfSites);
            bufferedOutput.printf(";\tSurrogate=%s\n", dcName);
            bufferedOutput.printf(";\tShepard=%s\n", shepard);
            bufferedOutput.printf(";\tthreads=%s\n;\n", concurrentSessions);

            Workload.populate(SwiftAdServiceMain.numCategories, SwiftAdServiceMain.adsPerCategory,
                    SwiftAdServiceMain.maxViewCount);

            if (!shepard.isEmpty())
                Shepard.sheepJoinHerd(shepard);

            // Kick off all sessions, throughput is limited by
            // concurrentSessions.
            final ExecutorService sessionsExecutor = Executors.newFixedThreadPool(concurrentSessions,
                    Threading.factory("App"));

            System.err.println("Spawning session threads.");
            for (int i = 0; i < concurrentSessions; i++) {
                final int sessionId = i;
                final Workload commands = Workload.doMixed(site, SwiftAdServiceMain.numCategories,
                        SwiftAdServiceMain.adsPerCategory, SwiftAdServiceMain.opGroups,
                        SwiftAdServiceMain.sameCategoryFreq, numberOfSites);

                sessionsExecutor.execute(new Runnable() {
                    public void run() {
                        // Randomize startup to avoid clients running all at the
                        // same time; causes problems akin to DDOS symptoms.
                        Threading.sleep(Sys.rg.nextInt(1000));
                        SwiftAdServiceMain.runClientSession(sessionId, commands, false);
                    }
                });
            }

            // smd - report client progress every 1 seconds...
            new PeriodicTask(0.0, 1.0) {
                String prev = "";

                public void run() {
                    int done = commandsDone.get();
                    int total = totalCommands.get();
                    String curr = String.format("--->DONE: %.1f%%, %d/%d\n", 100.0 * done / total, done, total);
                    if (!curr.equals(prev)) {
                        System.err.println(curr);
                        prev = curr;
                    }
                }
            };

            // Wait for all sessions.
            sessionsExecutor.shutdown();
            try {
                sessionsExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.err.println("Session threads completed.");
        }
        System.exit(0);
    }

    protected static void exitWithUsage() {
        System.err.println("Usage 1: init <number_of_users>");
        System.err.println("Usage 2: run <surrogate addr> <concurrent_sessions>");
        System.exit(1);
    }
}
