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

import java.io.PrintStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import swift.application.social.Commands;
import swift.application.social.Workload;
import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.shepard.Shepard;
import sys.utils.Args;
import sys.utils.IP;
import sys.utils.Props;

/**
 * Benchmark of SwiftSocial, based on data model derived from WaltSocial
 * prototype [Sovran et al. SOSP 2011].
 * <p>
 * Runs in parallel SwiftSocial sessions from the provided file. Sessions can be
 * distributed among different instances by specifying sessions range.
 */
public class SwiftSocialBenchmarkClient {
    private static PrintStream bufferedOutput;

    static Endpoint socialServer;

    static AtomicInteger commandsDone = new AtomicInteger(0);
    static AtomicInteger totalCommands = new AtomicInteger(0);

    public static void main(String[] args) {
        if (args.length < 3) {
            exitWithUsage();
        }

        final String server = args[0];
        int site = Integer.valueOf(args[1]);
        int number_of_sites = Integer.valueOf(args[2]);
        int concurrentSessions = Integer.valueOf(args[3]);

        final String shepardAddress = Args.valueOf(args, "-shepard", "");

        sys.Sys.init();

        socialServer = Networking.resolve(server, SwiftSocialBenchmarkServer.PORT);

        bufferedOutput = new PrintStream(System.out, false);
        bufferedOutput.println("session_id,command,command_exec_time,time");

        Props.parseFile("swiftsocial", bufferedOutput);

        int numUsers = Props.intValue("swiftsocial.numUsers", 25000);
        int userFriends = Props.intValue("swiftsocial.userFriends", 25);
        int biasedOps = Props.intValue("swiftsocial.biasedOps", 9);
        int randomOps = Props.intValue("swiftsocial.biasedOps", 1);
        int opGroups = Props.intValue("swiftsocial.opGroups", 500);

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

    private static void exitWithUsage() {
        System.out.println("Usage 1: init <surrogate addr> <users filename>");
        System.out.println("With the last option being true, input is treated as list of users to populate db.");
        System.out.println("Without the last options, input is treated as list of sessions with commands to run.");
        System.out
                .println("Usage 2: run <surrogate addr> <commands filename> <isolation level> <cache policy> <cache time eviction ms> <subscribe updates (true|false)> <async commit (true|false)>");
        System.out.println("         <think time ms> <concurrent sessions>");
        System.out.println("With the last option being true, input is treated as list of users to populate db.");
        System.out.println("Without the last options, input is treated as list of sessions with commands to run.");
        System.exit(1);
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
