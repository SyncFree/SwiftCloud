package swift.deployment

import java.util.concurrent.atomic.AtomicInteger
import static swift.deployment.Tools.*

class SwiftYCSB extends SwiftBase {
    static String YCSB_DRIVER = "swift.application.ycsb.SwiftMapPerKeyClient"
    static String INITDB_CMD = "-cp swiftcloud.jar -Djava.util.logging.config.file=logging.properties com.yahoo.ycsb.Client -db " + YCSB_DRIVER
    static String YCSB_CMD = "-Xincgc -cp swiftcloud.jar -Djava.util.logging.config.file=logging.properties com.yahoo.ycsb.Client -db " + YCSB_DRIVER

    static int initDB( String client, String server, String config, int threads = 1, String heap = "512m") {
        println "CLIENT: " + client + " SERVER: " + server + " CONFIG: " + config

        def cmd = INITDB_CMD + " -load -s -P " + config + " -p swift.hostname=" + server + " -threads " + threads +" "
        def res = rshC( client, swift_app_cmd_nostdout("-Xmx" + heap, cmd, "initdb-stderr.txt", "initdb-stdout.txt")).waitFor()
        println "OK.\n"
        return res
    }

    static void runClients(List scoutGroups, String config, String shepard, int threads = 1, String heap ="512m" ) {
        def hosts = []

        scoutGroups.each{ hosts += it.all() }

        println hosts

        AtomicInteger n = new AtomicInteger();
        def resHandler = { host, res ->
            def str = n.incrementAndGet() + "/" + hosts.size() + (res < 1 ? " [ OK ]" : " [FAILED]") + " : " + host
            println str
        }

        scoutGroups.each { grp ->
            Thread.startDaemon {
                def cmd = { host ->
                    int index = hosts.indexOf( host );
                    // Use tail, because ofr for unknown reason "2> (tee scout-stderr 1>&2)" closes tee prematurely
                    def res = "nohup java -Xmx" + heap + " " + YCSB_CMD + " -t -s -P " + config  + " -p swift.hostname=" + grp.dc.surrogates[index % grp.dc.surrogates.size()]
                    res += " -threads " + threads + " -shepard " + shepard + " 2> scout-stderr.txt > scout-stdout.txt  < /dev/null & sleep 1; tail -f scout-stderr.txt &"
                }
                grp.deploy( cmd, resHandler)
            }
        }
        // Parallel.rsh( clients, cmd, resHandler, true, 500000)
    }

    static final DEFAULT_PROPS = [
        'swift.cacheEvictionTimeMillis':'5000000',
        'swift.maxCommitBatchSize':'10',
        'swift.maxAsyncTransactionsQueued':'50',
        'swift.cacheSize':'0',
        'swift.asyncCommit':'false',
        'swift.notifications':'false',
        'swift.causalNotifications':'false',
        'swift.cachePolicy':'CACHED',
        'swift.isolationLevel':'SNAPSHOT_ISOLATION',
        'swift.computeMetadataStatistics':'false',
        'swift.reportEveryOperation':'false',
    ]

    // TODO: use properties file?
    static final WORKLOAD_A = ['recordcount':'1000',
        'operationcount':'1000',
        'workload':'com.yahoo.ycsb.workloads.CoreWorkload',
        'readallfields':'true',
        'readproportion':'0.5',
        'updateproportion':'0.5',
        'scanproportion':'0',
        'insertproportion':'0',
        'requestdistribution':'zipfian'
    ]
}