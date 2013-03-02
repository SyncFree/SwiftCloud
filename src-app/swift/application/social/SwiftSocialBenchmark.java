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

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
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
 * Benchmark of SwiftSocial, based on data model derived from WaltSocial
 * prototype [Sovran et al. SOSP 2011].
 * <p>
 * Runs in parallel SwiftSocial sessions from the provided file. Sessions can be
 * distributed among different instances by specifying sessions range.
 */
public class SwiftSocialBenchmark extends SwiftSocialMain {
    private static String shepard;

    public static void main(String[] args) {

        final String command = args[0];
        dcName = shepard = args[1];
        sys.Sys.init();

        if (dcName.equals("@")) {
            dcName = "localhost";
            DCServer.main(new String[] { dcName });
            DCSequencerServer.main(new String[] { "-name", dcName });
        }

        if ((command.equals("init") && args.length == 2) || command.equals("both")) {

            Props.parseFile("swiftsocial", System.out);
            final SwiftOptions options = new SwiftOptions(dcName, DCConstants.SURROGATE_PORT);
            options.setConcurrentOpenTransactions(true);

            System.out.println("Populating db with users...");

            final int numUsers = Props.intValue("swiftsocial.numUsers", 1000);
            List<String> users = Workload.populate(numUsers);

            final int PARTITION_SIZE = 1000;
            int partitions = users.size() / PARTITION_SIZE + (users.size() % PARTITION_SIZE > 0 ? 1 : 0);
            ExecutorService pool = Executors.newFixedThreadPool(4);

            final AtomicInteger counter = new AtomicInteger(0);
            for (int i = 0; i < partitions; i++) {
                int lo = i * PARTITION_SIZE, hi = (i + 1) * PARTITION_SIZE;
                final List<String> partition = users.subList(lo, Math.min(hi, users.size()));
                pool.execute(new Runnable() {
                    public void run() {
                        SwiftSocialMain.initUsers(options, partition, counter, numUsers);
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
            System.out.println("\nFinished populating db with users.");
        }
        if ((command.equals("run") && args.length >= 3) || command.equals("both")) {

            // IO.redirect("stdout.txt", "stderr.txt");
            System.err.println(IP.localHostname() + "/ starting...");

            int[] partitions;
            int concurrentSessions = Integer.valueOf(args[2]);
            try {
                partitions = new int[] { Integer.valueOf(args[3]), Integer.valueOf(args[4]) };
            } catch (Exception x) {
                partitions = parsePartitionsFile(Args.valueOf(args, "-partitions", "partitions.txt"));
            }
            int site = partitions[0];
            int numberOfSites = partitions[1];

            List<String> servers = Args.subList(args, "-servers");
            dcName = ClosestDomain.closest2Domain(servers, site);

            System.err.println(IP.localHostAddress() + " connecting to: " + dcName);

            SwiftSocialMain.init();

            Workload.populate(SwiftSocialMain.numUsers);

            new Shepard().joinHerd(shepard);

            // Kick off all sessions, throughput is limited by
            // concurrentSessions.
            final ExecutorService sessionsExecutor = Executors.newFixedThreadPool(concurrentSessions,
                    Threading.factory("App"));

            System.err.println("Spawning session threads.");
            for (int i = 0; i < concurrentSessions; i++) {
                final int sessionId = i;
                final Workload commands = Workload.doMixed(site, SwiftSocialMain.userFriends,
                        SwiftSocialMain.biasedOps, SwiftSocialMain.randomOps, SwiftSocialMain.opGroups, numberOfSites);

                totalCommands.addAndGet(commands.size());

                sessionsExecutor.execute(new Runnable() {
                    public void run() {
                        // Randomize startup to avoid clients running all at the
                        // same time; causes problems akin to DDOS symptoms.
                        Threading.sleep(Sys.rg.nextInt(1000));
                        SwiftSocialMain.runClientSession(sessionId, commands, false);
                    }
                });
            }

            // smd - report client progress every 10 seconds...
            new PeriodicTask(0.0, 1.0) {
                String prev = "";

                public void run() {
                    String curr = String.format("--->DONE: %.1f%%\n", 100.0 * commandsDone.get() / totalCommands.get());
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

    static protected int[] parsePartitionsFile(String partitions) {
        int[] res = new int[] { -1, 0 };
        try {
            String line;
            String hostname = IP.localHostname(), address = IP.localHostAddressString();
            String domain = domainName(hostname);
            BufferedReader br = new BufferedReader(new FileReader(partitions));
            while ((line = br.readLine()) != null) {
                if (res[0] < 0)
                    if (hostname.equals(line))
                        res[0] = res[1];
                    else if (line.endsWith(domain) && address.equals(IP.addressString(line)))
                        res[0] = res[1];

                res[1]++;
            }
            br.close();
        } catch (Throwable t) {
            t.printStackTrace();
        }
        System.err.printf(IP.localHostname() + " partitions #%s, total #%s\n", res[0], res[1]);
        return res;
    }

    static protected String domainName(String hostname) {
        int i = hostname.indexOf('.');
        return i < 0 ? hostname : hostname.substring(i + 1);
    }
}
