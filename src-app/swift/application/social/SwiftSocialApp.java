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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
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
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.utils.SafeLog;
import swift.utils.SafeLog.ReportType;
import sys.utils.Args;
import sys.utils.Progress;
import sys.utils.Props;
import sys.utils.Threading;

/**
 * Executing SwiftSocial operations, based on data model of WaltSocial prototype
 * [Sovran et al. SOSP 2011].
 * <p>
 * Runs SwiftSocial workload that is generated on the fly.
 */

public class SwiftSocialApp {
    protected String server;
    protected IsolationLevel isolationLevel;
    protected CachePolicy cachePolicy;
    protected boolean subscribeUpdates;
    protected boolean asyncCommit;

    protected int thinkTime;
    protected int opsPerMs;
    protected int numUsers;
    protected int userFriends;
    protected int biasedOps;
    protected int randomOps;
    protected int opGroups;
    protected boolean recordPageViews;

    protected AtomicInteger commandsDone = new AtomicInteger(0);
    protected AtomicInteger totalCommands = new AtomicInteger(0);
    private Properties props;

    private String propFile;
    protected int targetOpsPerSec;
    private boolean bloatedCounters;

    public void init(String[] args) {
        System.err.println(Arrays.asList(args));
        propFile = Args.valueOf(args, "-props", "swiftsocial-test.props");
        server = Args.valueOf(args, "-servers", "localhost");
    }

    public void populateWorkloadFromConfig() {

        props = Props.parseFile("swiftsocial", propFile);
        SafeLog.configure(props);
        isolationLevel = IsolationLevel.valueOf(Props.get(props, "swift.isolationLevel"));
        cachePolicy = CachePolicy.valueOf(Props.get(props, "swift.cachePolicy"));
        subscribeUpdates = Props.boolValue(props, "swift.notifications", false);
        asyncCommit = Props.boolValue(props, "swift.asyncCommit", true);

        numUsers = Props.intValue(props, "swiftsocial.numUsers", 1000);
        userFriends = Props.intValue(props, "swiftsocial.userFriends", 25);
        biasedOps = Props.intValue(props, "swiftsocial.biasedOps", 9);
        randomOps = Props.intValue(props, "swiftsocial.randomOps", 1);
        opGroups = Props.intValue(props, "swiftsocial.opGroups", 500);
        recordPageViews = Props.boolValue(props, "swiftsocial.recordPageViews", false);
        thinkTime = Props.intValue(props, "swiftsocial.thinkTime", 1000);
        targetOpsPerSec = Props.intValue(props, "swiftsocial.targetOpsPerSec", -1);
        configBloatedCounters(props);

        Workload.generateUsers(numUsers);
    }

    protected void configBloatedCounters(Properties properties) {
        bloatedCounters = Props.boolValue(properties, "swift.bloatedCounters", false);
    }

    public Workload getWorkloadFromConfig(int site, int numberOfSites) {
        if (props == null)
            populateWorkloadFromConfig();
        return Workload.doMixed(site, userFriends, biasedOps, randomOps, opGroups, numberOfSites);
    }

    public SwiftSocialOps getSwiftSocial(final String sessionId) {
        final SwiftOptions options = new SwiftOptions(server, DCConstants.SURROGATE_PORT, props);
        SwiftSession swiftClient = SwiftImpl.newSingleSessionInstance(options, sessionId);
        SwiftSocialOps socialClient = new SwiftSocialOps(swiftClient, isolationLevel, cachePolicy, subscribeUpdates,
                asyncCommit, bloatedCounters);
        return socialClient;
    }

    void runClientSession(final String sessionId, final Workload commands, boolean loop4Ever, double opsPerMsTarget) {
        final SwiftSocialOps socialClient = getSwiftSocial(sessionId);

        totalCommands.addAndGet(commands.size());
        final long sessionStartTime = System.currentTimeMillis();
        int sessionOpsDone = 0;
        SafeLog.report(ReportType.APP_OP, sessionId, "INIT", 0);

        do
            for (String cmdLine : commands) {
                long txnStartTime = System.currentTimeMillis();
                final Commands cmd = Commands.extract(cmdLine);
                if (cmd == null) {
                    SafeLog.report(ReportType.APP_OP_FAILURE, sessionId, cmd, "unsupported_operation");
                    continue;
                }
                try {
                    runCommandLine(socialClient, cmd, cmdLine);
                    long txnEndTime = System.currentTimeMillis();
                    final long txnExecTime = txnEndTime - txnStartTime;
                    SafeLog.report(ReportType.APP_OP, sessionId, cmd, txnExecTime);
                } catch (NoSuchObjectException e) {
                    SafeLog.report(ReportType.APP_OP_FAILURE, sessionId, cmd, "object_not_found");
                } catch (NetworkException e) {
                    SafeLog.report(ReportType.APP_OP_FAILURE, sessionId, cmd, "network_failure");
                } catch (VersionNotFoundException e) {
                    SafeLog.report(ReportType.APP_OP_FAILURE, sessionId, cmd, "version_pruned");
                } catch (WrongTypeException e) {
                    SafeLog.report(ReportType.APP_OP_FAILURE, sessionId, cmd, "wrong_object_type");
                } catch (SwiftException e) {
                    SafeLog.report(ReportType.APP_OP_FAILURE, sessionId, cmd, "unknown");
                }

                commandsDone.incrementAndGet();
                sessionOpsDone++;
                // Throttling mechanism borrowed from YCSB's Client.java.
                if (opsPerMsTarget > 0) {
                    while (System.currentTimeMillis() - sessionStartTime < ((double) sessionOpsDone) / opsPerMsTarget) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            // do nothing.
                        }
                    }
                } else {
                    Threading.sleep(thinkTime);
                }
            }
        while (loop4Ever);

        socialClient.getSwift().stopScout(true);

        final long now = System.currentTimeMillis();
        final long sessionExecTime = now - sessionStartTime;
        SafeLog.report(ReportType.APP_OP, sessionId, "TOTAL", sessionExecTime);
        SafeLog.flush();
    }

    public void runCommandLine(SwiftSocialOps socialClient, Commands cmd, String cmdLine) throws SwiftException {
        String[] toks = cmdLine.split(";");
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
                socialClient.read(toks[1], new HashSet<Message>(), new HashSet<Message>(), recordPageViews);
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
            throw new SwiftException("Malformed command line " + cmdLine);
            // System.err.println("Exiting...");
            // System.exit(1);
        }
    }

    public void initUsers(SwiftOptions swiftOptions, final List<String> users, AtomicInteger counter, int total) {
        try {
            SwiftSession swiftClient = SwiftImpl.newSingleSessionInstance(swiftOptions);
            SwiftSocialOps client = new SwiftSocialOps(swiftClient, isolationLevel, cachePolicy, subscribeUpdates,
                    asyncCommit, bloatedCounters);

            TxnHandle txn = swiftClient.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, false);
            int txnSize = 0;
            // Initialize user data
            List<String> userData = users;
            for (String line : userData) {
                // Divide into smaller transactions.
                if (txnSize >= 100) {
                    txn.commit();
                    txn = swiftClient.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, false);
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
                System.err.printf("Done: %s\n", Progress.percentage(counter.incrementAndGet(), total));
            }
            // Commit the last batch
            if (!txn.getStatus().isTerminated()) {
                txn.commit();
            }
            swiftClient.stopScout(true);
        } catch (SwiftException e) {
            e.printStackTrace();
        }
    }

}
