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
package swift.application.social.cs;

import static sys.net.api.Networking.Networking;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import swift.application.social.Commands;
import swift.application.social.SwiftSocialBenchmark;
import swift.application.social.Workload;
import sys.ec2.ClosestDomain;
import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.shepard.Shepard;
import sys.utils.Args;
import sys.utils.IP;

/**
 * Benchmark of SwiftSocial, based on data model derived from WaltSocial
 * prototype [Sovran et al. SOSP 2011].
 * <p>
 * Runs in parallel SwiftSocial sessions from the provided file. Sessions can be
 * distributed among different instances by specifying sessions range.
 */
public class SwiftSocialBenchmarkClient extends SwiftSocialBenchmark {

    static Endpoint socialServer;

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("wrong number of parameters...i know not very helpful...");
        }
        sys.Sys.init();

        final String shepardAddress = Args.valueOf(args, "-shepard", "");
        int concurrentSessions = Args.valueOf(args, "-threads", 1);

        int[] partitions;
        try {
            partitions = new int[] { Integer.valueOf(args[3]), Integer.valueOf(args[4]) };
        } catch (Exception x) {
            partitions = parsePartitionsFile(Args.valueOf(args, "-partitions", "partitions.txt"));
        }
        int site = partitions[0];
        int number_of_sites = partitions[1];

        List<String> servers = Args.subList(args, "-servers");
        String server = ClosestDomain.closest2Domain(servers, site);

        init();

        socialServer = Networking.resolve(server, SwiftSocialBenchmarkServer.SCOUT_PORT);

        Workload.populate(numUsers);

        // Kick off all sessions, throughput is limited by
        // concurrentSessions.
        final ExecutorService sessionsExecutor = Executors.newFixedThreadPool(concurrentSessions);

        if (!shepardAddress.isEmpty())
            new Shepard().joinHerd(shepardAddress);

        System.err.println("Spawning session threads.");
        for (int i = 0; i < concurrentSessions; i++) {
            final int sessionId = i;
            final Workload commands = Workload.doMixed(site, userFriends, biasedOps, randomOps, opGroups,
                    number_of_sites);
            sessionsExecutor.execute(new Runnable() {
                public void run() {
                    boolean loop4ever = !shepardAddress.isEmpty();
                    runClientSession(sessionId, commands, loop4ever);
                }
            });
        }
        // Wait for all sessions.
        sessionsExecutor.shutdown();
        try {
            sessionsExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.err.println("Session threads completed.");
        System.exit(0);
    }

    private static void runClientSession(final int sessionId, final Workload commands, boolean loop4ever) {

        RpcEndpoint endpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toDefaultService();

        totalCommands.addAndGet(commands.size());
        final long sessionStartTime = System.currentTimeMillis();
        final String initSessionLog = String.format("%d,%s,%d,%d", -1, "INIT", 0, sessionStartTime);

        bufferedOutput.println(initSessionLog);
        do
            for (String cmdLine : commands) {
                String[] toks = cmdLine.split(";");
                final Commands cmd = Commands.valueOf(toks[0].toUpperCase());
                final long txnStartTime = System.currentTimeMillis();
                endpoint.send(socialServer, new Request(cmdLine), new RequestHandler() {
                    public void onReceive(Request m) {
                        final long now = System.currentTimeMillis();
                        final long txnExecTime = now - txnStartTime;
                        final String log = String.format("%d,%s,%d,%d", sessionId, cmd, txnExecTime, now);
                        bufferedOutput.println(log);
                        commandsDone.incrementAndGet();
                    }
                });

            }
        while (loop4ever);

        final long now = System.currentTimeMillis();
        final long sessionExecTime = now - sessionStartTime;
        bufferedOutput.println(String.format("%d,%s,%d,%d", sessionId, "TOTAL", sessionExecTime, now));
        bufferedOutput.flush();
        System.err.println("> " + IP.localHostname() + " all sessions completed...");
    }
}
