package swift.deployment

import static swift.deployment.Tools.*

abstract class SwiftBase {
    static boolean ENABLE_YOUR_KIT_PROFILER = false
    static String YOUR_KIT_PROFILER_JAVA_OPTION =  ENABLE_YOUR_KIT_PROFILER ? " -agentpath:yjp/bin/linux-x86-64/libyjpagent.so " : ""
    static String SURROGATE_CMD = "-Xincgc -cp swiftcloud.jar -Djava.util.logging.config.file=logging.properties swift.dc.DCServer"
    static String SEQUENCER_CMD = "-Xincgc -cp swiftcloud.jar -Djava.util.logging.config.file=logging.properties swift.dc.DCSequencerServer"
    static String SHEPARD_CMD = "-cp swiftcloud.jar -Djava.util.logging.config.file=logging.properties sys.herd.Shepard"

    static final DEFAULT_PROPS = [
        'swift.cacheEvictionTimeMillis':'5000000',
        'swift.maxCommitBatchSize':'10',
        'swift.maxAsyncTransactionsQueued':'50',
        'swift.cacheSize':'0',
        'swift.asyncCommit':'false',
        'swift.notifications':'false',
        'swift.cacheUpdateProtocol':'NO_CACHE_OR_UNCOORDINATED',
        'swift.cachePolicy':'CACHED',
        'swift.isolationLevel':'SNAPSHOT_ISOLATION',
        'swift.reports':'APP_OP',
    ]

    static CACHING_NOTIFICATIONS_PROPS = ['swift.cacheSize':'256',
        'swift.asyncCommit':'true',
        'swift.notifications':'true',
        'swift.cacheUpdateProtocol':'CAUSAL_NOTIFICATIONS_STREAM']
    static CACHING_PERIODIC_REFRESH_PROPS = ['swift.cacheSize':'256',
        'swift.asyncCommit':'true',
        'swift.notifications':'false',
        'swift.cacheUpdateProtocol':'CAUSAL_PERIODIC_REFRESH',
        'swift.cacheRefreshPeriodMillis' : '1000']
    static NO_CACHING_NOTIFICATIONS_PROPS = [
        'swift.cacheSize':'0',
        'swift.asyncCommit':'false',
        'swift.notifications':'false',
        'swift.cacheUpdateProtocol':'NO_CACHE_OR_UNCOORDINATED']

    static MODES = [
        'refresh-frequent' : (CACHING_PERIODIC_REFRESH_PROPS + ['swift.cacheRefreshPeriodMillis' : '1000']),
        'refresh-infrequent': (CACHING_PERIODIC_REFRESH_PROPS + ['swift.cacheRefreshPeriodMillis' : '10000']),
        'notifications-frequent': CACHING_NOTIFICATIONS_PROPS  + ['swift.notificationPeriodMillis':'1000'],
        'no-caching' : NO_CACHING_NOTIFICATIONS_PROPS,
        'notifications-infrequent': CACHING_NOTIFICATIONS_PROPS + ['swift.notificationPeriodMillis':'10000'],
        'notifications-frequent-practi': CACHING_NOTIFICATIONS_PROPS + ['swift.notificationPeriodMillis':'10000', 'swift.notificationsFakePracti':'true'],
    ]

    static String swift_app_cmd( String heap, String exec, String stderr, String stdout ) {
        return "java " + YOUR_KIT_PROFILER_JAVA_OPTION + heap + " " + exec + "2> >(tee " + stderr + " 1>&2) > >(tee " + stdout + ")"
    }

    static String swift_app_cmd_nostdout( String heap, String exec, String stderr, String stdout )  {
        return "java " + YOUR_KIT_PROFILER_JAVA_OPTION + heap + " " + exec +  "2> >(tee " + stderr+ " 1>&2) > " + stdout
    }

    static String swift_app_cmd_nooutput( String heap, String exec, String stderr, String stdout )  {
        return "java " + YOUR_KIT_PROFILER_JAVA_OPTION + heap + " " + exec +  "2> " + stderr+ " > " + stdout
    }

    static String sequencerCmd( String siteId, String shepard, List servers, List otherSequencers, String extraArgs) {
        def res  = SEQUENCER_CMD + " -name " + siteId + " -shepard " + shepard + " -servers "
        servers.each { res += it + " "}
        res += "-sequencers "
        otherSequencers.each { res += it + " "}
        return res + extraArgs + " "
    }

    static String surrogateCmd( String siteId, String shepard, String sequencer, List otherSurrogates, String extraArgs ) {
        def res  = SURROGATE_CMD + " -name " + siteId  + " -shepard " + shepard + " -sequencer " + sequencer + " "
        res += "-surrogates "
        otherSurrogates.each { res += it + " "}
        return res + extraArgs + " "
    }

    static void runEachAsSequencer( List sequencers, List surrogates, String seqHeap) {
        println "==== STARTING DATACENTER SEQUENCERS ===="

        sequencers.each { host ->
            def sequencer = host
            def other_sequencers = sequencers.clone() - host
            def name = siteId(sequencers.indexOf(host))
            rshC(sequencer, swift_app_cmd( "-Xms"+seqHeap, sequencerCmd(name, surrogates, other_sequencers), "seq-stderr.txt" , "seq-stdout.txt"))
        }
        println "\nOK"
    }

    static void runEachAsSurrogate( List surrogates, String sequencer, String heap) {
        println "==== STARTING DATACENTER SURROGATES ===="

        surrogates.each { host ->
            rshC(host, swift_app_cmd_nostdout( "-Xms"+heap, surrogateCmd( sequencer ), "sur-stderr.txt", "sur-stdout.txt" ))
        }
        println "\nOK"
    }

    static void runEachAsDatacentre( List datacentres, String seqHeap, String surHeap ) {
        println "==== STARTING DATACENTER SERVERS ===="

        datacentres.each {
            def srv = it
            def surrogate = srv
            def sequencer = srv
            def other_sequencers = datacentres.clone() - srv
            def name = "X" + sequencers.indexOf(host)
            rshC(sequencer, swift_app_cmd( "-Xms"+seqHeap, sequencerCmd(name, [srv], other_sequencers), "seq-stderr.txt", "seq-stdout.txt" ))
            rshC(surrogate, swift_app_cmd( "-Xms"+surHeap, surrogateCmd( sequencer ), "sur-stderr.txt", "sur-stdout.txt" ))
            i++;
        }
        println "\nOK"
    }


    static def runShepard( host, duration, pattern ) {
        def queue = new java.util.concurrent.SynchronousQueue<?>()
        println "==== STARTING SHEPARD @ " + host + " DURATION: " + duration

        def cmd = SHEPARD_CMD + " -duration " + (int)duration + " "
        Process proc = rsh( host, swift_app_cmd("-Xmx64m", cmd, "shep-stdout.txt", "shep-stderr.txt") );
        Thread.start {
            proc.errorStream.withReader {
                String line;
                while ( (line = it.readLine() ) != null ) {
                    println line
                    if( line.contains( pattern ) ) {
                        queue.offer( proc )
                        return true
                    }
                }
            }
        }
        println "\nOK"
        return queue
    }

    static File genPropsFile(Map props, Map defaultProps = [:]) {
        File f = File.createTempFile("swif-", ".props")
        PrintWriter pw = f.newPrintWriter()
        (defaultProps + props).each { k, v ->
            pw.printf("%s=%s\n", k, v);
        }
        pw.close()
        return f
    }

    static String genDCServerPropArgs(Map props) {
        def result = ""
        props.each { k, v ->
            result += " -prop:" + k + " " + v
        }
        result += " "
        return result
    }

    def scouts
    def shepardAddr
    def allMachines

    int dbSize = 100000
    int threads = 10

    def pruningIntervalMillis = 60000
    static int DEFAULT_NOTIFICATIONS_PERIOD_MS = 1000
    def notificationsPeriodMillis = DEFAULT_NOTIFICATIONS_PERIOD_MS
    def dcProps = ['swift.reports':'DATABASE_TABLE_SIZE,IDEMPOTENCE_GUARD_SIZE',
        'swift.notificationsFakePracti' : 'false',
        'swift.notificationsDeltaVectors' : 'false',
    ]
    def mode = CACHING_NOTIFICATIONS_PROPS
    public void setMode(def newMode) {
        mode = newMode
        if (mode.containsKey('swift.notificationsFakePracti')) {
            dcProps['swift.notificationsFakePracti'] = mode['swift.notificationsFakePracti']
        } else {
            dcProps.remove('swift.notificationsFakePracti')
        }
        if (mode.containsKey('swift.notificationPeriodMillis')) {
            notificationsPeriodMillis = Integer.parseInt(mode['swift.notificationPeriodMillis'])
        } else {
            notificationsPeriodMillis = DEFAULT_NOTIFICATIONS_PERIOD_MS
        }
    }
    def duration = 600
    def durationShepardGrace = 12
    def interCmdDelay = 30
    def reports = [
        'APP_OP',
        'APP_OP_FAILURE',
        'METADATA',
        'STALENESS_YCSB_WRITE',
        'STALENESS_WRITE'
    ]
    // STALENESS_YCSB_READ,STALENESS_READ,STALENESS_CALIB

    def integratedDC = true
    def version = getGitCommitId()

    Map config

    protected SwiftBase() {
        scouts = (Topology.scouts()).unique()
        shepardAddr = Topology.datacenters[0].surrogates[0];
        allMachines = (Topology.allMachines() + shepardAddr).unique()
    }

    public void runExperiment(String outputDir) {
        generateConfig()
        prepareNodes()
        onControlC({
            pnuke(AllMachines, "java", 60)
            System.exit(1);
        })

        def shep = startShepard()
        startDCs()
        println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
        Sleep(interCmdDelay)
        runInitDB()
        println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
        Sleep(interCmdDelay)
        runClientsWithShepard(shep)
        collectResults(outputDir)
    }

    protected abstract void generateConfig()
    protected abstract void deployConfig()
    protected abstract void doRunClients()
    protected abstract void doInitDB()

    private prepareNodes() {
        pnuke(allMachines, "java", 60)
        println "==== BUILDING JAR for version " + version + "..."
        sh("ant -buildfile smd-jar-build.xml").waitFor()
        deployTo(allMachines, "swiftcloud.jar")
        deployTo(allMachines, "stuff/logging.properties", "logging.properties")
        deployConfig()
    }

    private def startShepard() {
        return SwiftBase.runShepard(shepardAddr, duration + durationShepardGrace, "Released" )
    }

    private startDCs() {
        if (!integratedDC) {
            println "==== LAUNCHING SEQUENCERS"
            Topology.datacenters.each { datacenter ->
                datacenter.deploySequencers(shepardAddr, "1024m" )
            }
            Sleep(10)
        }

        println "==== LAUNCHING SURROGATES"
        Topology.datacenters.each { datacenter ->
            if (integratedDC) {
                datacenter.deployIntegratedSurrogatesExtraArgs(shepardAddr, "-pruningMs " + pruningIntervalMillis + " -notificationsMs " + notificationsPeriodMillis + SwiftBase.genDCServerPropArgs(dcProps), "2048m")
            } else {
                datacenter.deploySurrogatesExtraArgs(shepardAddr, "-pruningMs " + pruningIntervalMillis + " -notificationsMs " + notificationsPeriodMillis + SwiftBase.genDCServerPropArgs(dcProps), "2048m")
            }
        }
    }

    private runClientsWithShepard(def shep) {
        doRunClients()

        println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
        shep.take()
        Countdown( "Max. remaining time: ", duration + interCmdDelay)
        pnuke(allMachines, "java", 60)
    }


    private collectResults(String dstDir) {
        pslurp(scouts, "scout-stdout.txt", dstDir, "scout-stdout.log", 300)
        pslurp(scouts, "scout-stderr.txt", dstDir, "scout-stderr.log", 300)
        Topology.datacenters.each { dc ->
            pslurp(dc.surrogates, "sur-stderr.txt", dstDir, "sur-stderr.log", 30)
            pslurp(dc.surrogates, "sur-stdout.txt", dstDir, "sur-stdout.log", 300)
            if (!integratedDC) {
                pslurp(dc.sequencers, "seq-stderr.txt", dstDir, "seq-stderr.log", 30)
                pslurp(dc.sequencers, "seq-stdout.txt", dstDir, "seq-stdout.log", 30)
            }
        }
        def configFile = new File(dstDir, "config")
        configFile.createNewFile()
        configFile.withWriter { out ->
            out.writeLine(config.toString())
        }

        def stats = exec([
            "/bin/bash",
            "-c",
            "wc " + dstDir + "/*/*"
        ])

        exec([
            "tar",
            "-czf",
            dstDir+".tar.gz",
            dstDir
        ]).waitFor()

        stats.waitFor()

        exec([
            "rm",
            "-Rf",
            dstDir
        ]).waitFor()
    }

    private void runInitDB() {
        println "==== INITIALIZING DATABASE ===="
        doInitDB()
    }
}
