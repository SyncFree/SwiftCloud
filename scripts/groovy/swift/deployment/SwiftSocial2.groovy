package swift.deployment

import java.util.concurrent.atomic.AtomicInteger
import static swift.deployment.Tools.*
import static swift.deployment.Topology.*

class SwiftSocial2 extends SwiftBase {
    static String INITDB_CMD = "-cp swiftcloud.jar -Djava.util.logging.config.file=logging.properties swift.application.social.SwiftSocialBenchmark"
    static String SCOUT_CMD = "-Xincgc -cp swiftcloud.jar -Xincgc -Djava.util.logging.config.file=logging.properties swift.application.social.SwiftSocialBenchmark"

    static String CS_SCOUT_CMD = "-Xincgc -cp swiftcloud.jar -Xincgc -Djava.util.logging.config.file=logging.properties swift.application.social.cs.SwiftSocialBenchmarkServer"
    static String CS_ENDCLIENT_CMD = "-Xincgc -cp swiftcloud.jar -Djava.util.logging.config.file=logging.properties swift.application.social.cs.SwiftSocialBenchmarkClient"

    static int initDB( String client, String server, String config, String heap = "512m") {
        println "CLIENT: " + client + " SERVER: " + server + " CONFIG: " + config

        def cmd = "-Dswiftsocial=" + config + " " + INITDB_CMD + " init -servers " + server + " "
        def res = rshC( client, swift_app_cmd_nostdout("-Xmx" + heap, cmd, "initdb-stderr.txt", "initdb-stdout.txt")).waitFor()
        println "OK.\n"
        return res
    }


    static int prepareDB( String client, String server, String config, int threads, String heap = "512m") {
        println "CLIENT: " + client + " SERVER: " + server + " CONFIG: " + config

        def cmd = "-Dswiftsocial=" + config + " " + INITDB_CMD + " -prepareDB -threads " + threads + " "
        def res = rshC( client, swift_app_cmd("-Xmx" + heap, cmd, "prepdb-stdout.txt", "prepdb-stderr.txt")).waitFor()
        println "OK.\n"
        return res
    }

    static void runScouts( List scoutGroups, String config, String shepard, int threads, String heap ="512m" ) {
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
                    String partition = index + "/" + hosts.size()
                    def res = "nohup java " + SwiftBase.YOUR_KIT_PROFILER_JAVA_OPTION + "-Xmx" + heap + " -Dswiftsocial=" + config + " " + SCOUT_CMD + " run -shepard " + shepard + " -threads " + threads + " -partition " + partition + " -servers "
                    res += " " + grp.dc.surrogates[index % grp.dc.surrogates.size()]
                    res += " > scout-stdout.txt 2> scout-stderr.txt < /dev/null & sleep 1; tail -f scout-stderr.txt &"
                    return res;
                }
                grp.deploy( cmd, resHandler)
            }
        }
    }

    static final WORKLOAD_VIEWS_COUNTER = [
        'swiftsocial.numUsers':'1000',
        'swiftsocial.userFriends':'25',
        'swiftsocial.biasedOps':'9',
        'swiftsocial.randomOps':'1',
        'swiftsocial.opGroups':'10000',
        'swiftsocial.recordPageViews':'true',
        'swiftsocial.thinkTime':'0',
        'swiftsocial.targetOpsPerSec':'-1',
        'swiftsocial.recordPageViews' : 'true'
    ]

    static final WORKLOAD_NO_VIEWS_COUNTER = [
        'swiftsocial.numUsers':'1000',
        'swiftsocial.userFriends':'25',
        'swiftsocial.biasedOps':'9',
        'swiftsocial.randomOps':'1',
        'swiftsocial.opGroups':'10000',
        'swiftsocial.recordPageViews':'true',
        'swiftsocial.thinkTime':'0',
        'swiftsocial.targetOpsPerSec':'-1',
        'swiftsocial.recordPageViews' : 'false'
    ]

    static WORKLOADS= [
        'workload-social-views-counter' : WORKLOAD_VIEWS_COUNTER,
        'workload-social-no-views-counter' : WORKLOAD_NO_VIEWS_COUNTER
    ]

    // Two alternative mechanism to throttle the target throughput:
    // Low level think time between each operation.
    def thinkTime = 0
    // If set, takes priority over think time:
    def incomingOpPerSecLimit = -1
    def baseWorkload = WORKLOAD_NO_VIEWS_COUNTER

    def swiftSocialProps
    def swiftSocialPropsPath

    public SwiftSocial2() {
        super()
    }

    protected void generateConfig() {
        def workload = baseWorkload + ['swiftsocial.numUsers':dbSize.toString(),
            'swiftsocial.thinkTime': thinkTime.toString(),
            'swiftsocial.targetOpsPerSec' : ((Integer) (incomingOpPerSecLimit / scouts.size())).toString(),
        ]
        swiftSocialProps = DEFAULT_PROPS + workload + ['swift.reports' : reports.join(',')] + mode
        swiftSocialPropsPath = "swiftsocial.properties"

        config = properties + ['workload': workload, 'swiftSocialProps': swiftSocialProps]
        println config
    }

    protected void deployConfig() {
        deployTo(allMachines, genPropsFile(swiftSocialProps).absolutePath, swiftSocialPropsPath)
    }

    protected void doInitDB() {
        def initDbDc = Topology.datacenters[0].surrogates[0]
        def initDbClient  = Topology.datacenters[0].sequencers[0]

        SwiftSocial2.initDB(initDbClient, initDbDc, swiftSocialPropsPath, "1024m")
    }

    protected void doRunClients() {
        SwiftSocial2.runScouts(Topology.scoutGroups, swiftSocialPropsPath, shepardAddr, threads, "3072m")
    }
}
