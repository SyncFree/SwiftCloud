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

import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.dc.DCConstants;
import swift.exceptions.SwiftException;
import sys.utils.Args;
import sys.utils.Progress;
import sys.utils.Props;
import sys.utils.Threading;

/**
 * Executing SwiftSocial operations, based on data model of WaltSocial prototype
 * [Sovran et al. SOSP 2011].
 */
public class SwiftSocialApp {
    protected String server;
    protected IsolationLevel isolationLevel;
    protected CachePolicy cachePolicy;
    protected boolean subscribeUpdates;
    protected boolean asyncCommit;

    protected int thinkTime;
    protected int numUsers;
    protected int userFriends;
    protected int biasedOps;
    protected int randomOps;
    protected int opGroups;

    protected PrintStream bufferedOutput;

    protected AtomicInteger commandsDone = new AtomicInteger(0);
    protected AtomicInteger totalCommands = new AtomicInteger(0);
    private Properties props;

    public void init(String[] args) {
        server = Args.valueOf(args, "-servers", "localhost");
    }

    public List<String> populateWorkloadFromConfig() {

        props = Props.parseFile("swiftsocial", bufferedOutput, "swiftsocial-test.props");

        isolationLevel = IsolationLevel.valueOf(Props.get(props, "swift.isolationLevel"));
        cachePolicy = CachePolicy.valueOf(Props.get(props, "swift.cachePolicy"));
        subscribeUpdates = Props.boolValue(props, "swift.notifications", false);
        asyncCommit = Props.boolValue(props, "swift.asyncCommit", true);

        numUsers = Props.intValue(props, "swiftsocial.numUsers", 1000);
        userFriends = Props.intValue(props, "swiftsocial.userFriends", 25);
        biasedOps = Props.intValue(props, "swiftsocial.biasedOps", 9);
        randomOps = Props.intValue(props, "swiftsocial.randomOps", 1);
        opGroups = Props.intValue(props, "swiftsocial.opGroups", 500);
        thinkTime = Props.intValue(props, "swiftsocial.thinkTime", 1000);

        return Workload.populate(numUsers);
    }

    public Workload getWorkloadFromConfig(int site, int numberOfSites) {
        if (props == null)
            populateWorkloadFromConfig();
        return Workload.doMixed(site, userFriends, biasedOps, randomOps, opGroups, numberOfSites);
    }

    public SwiftSocialOps getSwiftSocial() {
        final SwiftOptions options = new SwiftOptions(server, DCConstants.SURROGATE_PORT, props);
        SwiftSession swiftClient = SwiftImpl.newSingleSessionInstance(options);
        SwiftSocialOps socialClient = new SwiftSocialOps(swiftClient, isolationLevel, cachePolicy, subscribeUpdates,
                asyncCommit);
        return socialClient;
    }

    void runClientSession(final int sessionId, final Workload commands, boolean loop4Ever) {
        final SwiftSocialOps socialClient = getSwiftSocial();

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

    public Commands runCommandLine(SwiftSocialOps socialClient, String cmdLine) {
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

    public void initUsers(SwiftOptions swiftOptions, final List<String> users, AtomicInteger counter, int total) {
        try {
            SwiftSession swiftClient = SwiftImpl.newSingleSessionInstance(swiftOptions);
            SwiftSocialOps client = new SwiftSocialOps(swiftClient, isolationLevel, cachePolicy, subscribeUpdates,
                    asyncCommit);

            TxnHandle txn = swiftClient.beginTxn(IsolationLevel.REPEATABLE_READS, CachePolicy.CACHED, false);
            int txnSize = 0;
            // Initialize user data
            List<String> userData = users;
            for (String line : userData) {
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
                System.out.printf("\rDone: %s", Progress.percentage(counter.incrementAndGet(), total));
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
