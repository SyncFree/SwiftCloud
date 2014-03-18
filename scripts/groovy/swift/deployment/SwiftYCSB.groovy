package swift.deployment

import java.util.concurrent.atomic.AtomicInteger
import static swift.deployment.Tools.*

class SwiftYCSB extends SwiftBase {
    static String INITDB_CMD = "-cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties com.yahoo.ycsb.Client -db swift.application.ycsb.SwiftRegisterPerFieldClient"
    static String YCSB_CMD = "-Xincgc -cp swiftcloud.jar -Xincgc -Djava.util.logging.config.file=all_logging.properties com.yahoo.ycsb.Client -db swift.application.ycsb.SwiftRegisterPerFieldClient"

    static int initDB( String client, String server, String config, int threads = 1, String heap = "512m") {
        println "CLIENT: " + client + " SERVER: " + server + " CONFIG: " + config

        def cmd = "-Dswiftcloud.hostname=" + server + " " + INITDB_CMD + " -load -s -P " + config + " -threads " + threads +" "
        def res = rshC( client, swift_app_cmd("-Xmx" + heap, cmd, "initdb-stdout.txt", "initdb-stderr.txt")).waitFor()
        println "OK.\n"
        return res
    }

    static void runClients(List clients, String server, String config, int threads = 1, String heap ="512m" ) {
        def cmd = { host ->
            // TODO: support multiple clients properly
            // String partition = scouts.indexOf( host ) + "/" + scouts.size()
            def res = "nohup java -Xmx" + heap + " -Dswift.hostname=" + server + " " + YCSB_CMD + " -t -s -P " + config + " -threads " + threads +" "
            res += "> scout-stdout.txt 2> scout-stderr.txt < /dev/null &"
            return res;
        }

        AtomicInteger n = new AtomicInteger();
        def resHandler = { host, res ->
            def str = n.incrementAndGet() + "/" + clients.size() + (res < 1 ? " [ OK ]" : " [FAILED]") + " : " + host
            println str
        }
        Parallel.rsh( clients, cmd, resHandler, true, 500000)
    }

    static final DEFAULT_PROPS = [
        'swift.cacheEvictionTimeMillis':'3600000',
        'swift.maxCommitBatchSize':'10',
        'swift.maxAsyncTransactionsQueued':'50',
        'swift.cacheSize':'1',
        'swift.asyncCommit':'false',
        'swift.notifications':'false',
        'swift.cachePolicy':'STRICTLY_MOST_RECENT',
        'swift.isolationLevel':'SNAPSHOT_ISOLATION'
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