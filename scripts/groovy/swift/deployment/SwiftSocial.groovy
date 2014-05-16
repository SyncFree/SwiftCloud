package swift.deployment

import java.util.concurrent.atomic.AtomicInteger
import static swift.deployment.Tools.*

class SwiftSocial extends SwiftBase {
    static String INITDB_CMD = "-cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties swift.application.social.SwiftSocialBenchmark"
    static String SCOUT_CMD = "-Xincgc -cp swiftcloud.jar -Xincgc -Djava.util.logging.config.file=all_logging.properties swift.application.social.SwiftSocialBenchmark"

    static String CS_SCOUT_CMD = "-Xincgc -cp swiftcloud.jar -Xincgc -Djava.util.logging.config.file=all_logging.properties swift.application.social.cs.SwiftSocialBenchmarkServer"
    static String CS_ENDCLIENT_CMD = "-Xincgc -cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties swift.application.social.cs.SwiftSocialBenchmarkClient"

    static int initDB( String client, String server, String config, String heap = "512m") {
        println "CLIENT: " + client + " SERVER: " + server + " CONFIG: " + config

        def cmd = "-Dswiftsocial=" + config + " " + INITDB_CMD + " init -servers " + server + " "
        def res = rshC( client, swift_app_cmd("-Xmx" + heap, cmd, "initdb-stdout.txt", "initdb-stderr.txt")).waitFor()
        println "OK.\n"
        return res
    }

    static void runStandaloneScouts( List scouts, Map scoutsToServersMap, String config, String shepard, int threads, String heap ="512m" ) {
        def cmd = { scout ->
            String partition = scouts.indexOf( scout ) + "/" + scouts.size()
            def res = "nohup java -Xmx" + heap + " -Dswiftsocial=" + config + " " + SCOUT_CMD + " run -shepard " + shepard + " -threads " + threads + " -partition " + partition + " -servers "
            scoutsToServersMap[scout].each { res += it + " "}
            res += "> scout-stdout.txt 2> scout-stderr.txt < /dev/null &"
            return res;
        }

        AtomicInteger n = new AtomicInteger();
        def resHandler = { host, res ->
            def str = n.incrementAndGet() + "/" + scouts.size() + (res < 1 ? " [ OK ]" : " [FAILED]") + " : " + host
            println str
        }
        Parallel.rsh( scouts, cmd, resHandler, true, 500000)
    }

    static void runStandaloneScoutFailOver( List scouts, List servers, String config, String shepard, int threads, String heap ="512m" ) {
        def cmd = { host ->
            String partition = scouts.indexOf( host ) + "/" + scouts.size()
            def res = "nohup java -Xmx" + heap + " -Dswiftsocial=" + config + " " + SCOUT_CMD + " run -shepard " + shepard + " -threads " + threads + " -partition " + partition + " -servers "
            servers.each { res += it + ","}
            res += "> scout-stdout.txt 2> scout-stderr.txt < /dev/null &"
            return res;
        }

        AtomicInteger n = new AtomicInteger();
        def resHandler = { host, res ->
            def str = n.incrementAndGet() + "/" + scouts.size() + (res < 1 ? " [ OK ]" : " [FAILED]") + " : " + host
            println str
        }
        Parallel.rsh( scouts, cmd, resHandler, true, 500000)
    }
    static void runCS_ServerScouts( int instances, List scouts, List servers, String config, String heap="512m") {

        AtomicInteger n = new AtomicInteger();
        def resHandler = { host, res ->
            def str = n.incrementAndGet() + "/" + scouts.size() + (res < 1 ? " [ OK ]" : " [FAILED]") + " : " + host
            println str
        }

        instances.times{ instance ->
            def cmd = { _ ->
                def str = "nohup nice -n 10 java -Xmx" + heap + " -Dswiftsocial=" + config + " " + CS_SCOUT_CMD + " -instance " + instance + " -servers "
                servers.each { str += it + " "}
                str += "> scout-stdout-"+ instance+".txt 2> scout-stderr-"+instance+".txt < /dev/null &"
            }
            Parallel.rsh( scouts, cmd, resHandler, true, 500000)
        }
    }

    static void runCS_EndClients( int instances, List clients, List scouts, String config, String shepard, int threads, String heap = "128m") {
        def cmd = { host ->
            String partition = clients.indexOf( host ) + "/" + clients.size()
            def res = "nohup java -Xmx" + heap + " -Dswiftsocial=" + config + " " + CS_ENDCLIENT_CMD + " -shepard " + shepard + " -threads " + threads + " -partition " + partition + " -instances " + instances + " -servers "
            scouts.each { res += it + " "}
            res += "> client-stdout.txt 2> client-stderr.txt < /dev/null &"
            return res
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
        'swift.cacheSize':'512',
        'swift.asyncCommit':'true',
        'swift.notifications':'true',
        'swift.cachePolicy':'CACHED',
        'swift.isolationLevel':'SNAPSHOT_ISOLATION',
        'swift.computeMetadataStatistics':'false',
        'swiftsocial.numUsers':'25000',
        'swiftsocial.userFriends':'25',
        'swiftsocial.biasedOps':'9',
        'swiftsocial.randomOps':'1',
        'swiftsocial.opGroups':'10000',
        'swiftsocial.thinkTime':'0'
    ]
}