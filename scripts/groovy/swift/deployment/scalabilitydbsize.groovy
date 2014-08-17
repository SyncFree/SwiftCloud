#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.SwiftYCSB.*
import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(1);
})

INTEGRATED_DC = true

if (args.length != 5) {
    System.err.println "usage: runycsb.groovy <topology configuration file> <workload> <mode> <dbsize> <outputdir>"
    System.exit(1)
}

// TOPOLOGY CONFIGURATION
topologyDef = new File(args[0])
println "==== Loading topology definition from file " + topologyDef + "===="
evaluate(topologyDef)
Scouts = ( Topology.scouts() ).unique()
ShepardAddr = Topology.datacenters[0].surrogates[0];
AllMachines = ( Topology.allMachines() + ShepardAddr).unique()

// VARs
WorkloadName = args[1]
ModeName = args[2]
WORKLOADS= [
    'workloada-uniform' : SwiftYCSB.WORKLOAD_A + ['requestdistribution': 'uniform'],
    'workloada' : SwiftYCSB.WORKLOAD_A,
    'workloadb-uniform' : SwiftYCSB.WORKLOAD_B + ['requestdistribution': 'uniform'],
    'workloadb' : SwiftYCSB.WORKLOAD_B,
]
BaseWorkload = WORKLOADS[WorkloadName]
MODES = [
    'refresh-frequent' : (SwiftBase.CACHING_PERIODIC_REFRESH_PROPS + ['swift.cacheRefreshPeriodMillis' : '1000']),
    'refresh-infrequent': (SwiftBase.CACHING_PERIODIC_REFRESH_PROPS + ['swift.cacheRefreshPeriodMillis' : '10000']),
    'notifications-frequent': SwiftBase.CACHING_NOTIFICATIONS_PROPS  + ['swift.notificationPeriodMillis':'1000'],
    'no-caching' : SwiftBase.NO_CACHING_NOTIFICATIONS_PROPS,
    'notifications-infrequent': SwiftBase.CACHING_NOTIFICATIONS_PROPS + ['swift.notificationPeriodMillis':'10000'],
    'notifications-frequent-practi': SwiftBase.CACHING_NOTIFICATIONS_PROPS + ['swift.notificationPeriodMillis':'10000', 'swift.notificationsFakePracti':'true'],
]
DbSize = Integer.parseInt(args[3])
OutputDir = args[4]

Mode = MODES[ModeName]

// OPTIONS
OBJECTS_PER_CLIENT = 50
OPS_PER_CLIENT = 2.5
Clients = DbSize / OBJECTS_PER_CLIENT
IncomingOpPerSecLimit = (int) (OPS_PER_CLIENT * ((double) Clients))
Threads = Scouts.size()
OpsNum = 10000000
PruningIntervalMillis = 60000
NotificationsPeriodMillis = Mode.containsKey('swift.notificationPeriodMillis') ? Mode['swift.notificationPeriodMillis'] : '1000'
IncomingOpPerSecPerClientLimit = (int) (IncomingOpPerSecLimit / Scouts.size())

Duration = 600
DurationShepardGrace = 12
InterCmdDelay = 30

WORKLOAD = BaseWorkload + ['recordcount': DbSize.toString(), 'operationcount':OpsNum.toString(),
    'target':IncomingOpPerSecPerClientLimit,

    'localpoolfromglobaldistribution':'true',
    'localrequestdistribution':'uniform',
    'localrecordcount':'150',
    'localrequestproportion':'0.8',
]
// STALENESS_YCSB_READ,STALENESS_YCSB_WRITE,STALENESS_CALIB
REPORTS = ['swift.reports':'APP_OP,APP_OP_FAILURE,METADATA,STALENESS_YCSB_WRITE', 'swift.reportEveryOperation':'true']

DC_PROPS = ['swift.reports':'DATABASE_TABLE_SIZE,IDEMPOTENCE_GUARD_SIZE',
    'swift.notificationsFakePracti' : Mode.containsKey('swift.notificationsFakePracti')? Mode['swift.notificationsFakePracti'] : 'false',
    'swift.notificationsDeltaVectors' : 'false',
]
YCSB_PROPS = SwiftYCSB.DEFAULT_PROPS + WORKLOAD + REPORTS + Mode + ['maxexecutiontime' : Duration]

// Options for DB initialization
INIT_NO_REPORTS = ['swift.reports':'']
INIT_OPTIONS = SwiftBase.NO_CACHING_NOTIFICATIONS_PROPS
INIT_THREADS = 2

INIT_YCSB_PROPS = SwiftYCSB.DEFAULT_PROPS + WORKLOAD  + ['target':'1000000'] + INIT_NO_REPORTS+ INIT_OPTIONS

Version = getGitCommitId()
String config = getBinding().getVariables()
println config

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)


println "==== BUILDING JAR for version " + Version + "..."
sh("ant -buildfile smd-jar-build.xml").waitFor()
deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, "stuff/logging.properties", "logging.properties")
YCSBProps = "swiftycsb.properties"
deployTo(AllMachines, SwiftYCSB.genPropsFile(YCSB_PROPS).absolutePath, YCSBProps)
INITYCSBProps = "swiftycsb-init.properties"
deployTo(AllMachines, SwiftYCSB.genPropsFile(INIT_YCSB_PROPS).absolutePath, INITYCSBProps)

def shep = SwiftBase.runShepard( ShepardAddr, Duration + DurationShepardGrace, "Released" )

if (!INTEGRATED_DC) {
    println "==== LAUNCHING SEQUENCERS"
    Topology.datacenters.each { datacenter ->
        datacenter.deploySequencers(ShepardAddr, "1024m" )
    }
    Sleep(10)
}

println "==== LAUNCHING SURROGATES"
Topology.datacenters.each { datacenter ->
    if (INTEGRATED_DC) {
        datacenter.deployIntegratedSurrogatesExtraArgs(ShepardAddr, "-pruningMs " + PruningIntervalMillis + " -notificationsMs " + NotificationsPeriodMillis + SwiftBase.genDCServerPropArgs(DC_PROPS), "2048m")
    } else {
        datacenter.deploySurrogatesExtraArgs(ShepardAddr, "-pruningMs " + PruningIntervalMillis + " -notificationsMs " + NotificationsPeriodMillis + SwiftBase.genDCServerPropArgs(DC_PROPS), "2048m")
    }
}

println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
Sleep(InterCmdDelay)

println "==== INITIALIZING DATABASE ===="
def INIT_DB_DC = Topology.datacenters[0].surrogates[0]
def INIT_DB_CLIENT = Topology.datacenters[0].sequencers[0]

SwiftYCSB.initDB( INIT_DB_CLIENT, INIT_DB_DC, INITYCSBProps, INIT_THREADS)

println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
Sleep(InterCmdDelay)

SwiftYCSB.runClients(Topology.scoutGroups, YCSBProps, ShepardAddr, Threads, "2560m")

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Max. remaining time: ", Duration + InterCmdDelay)

pnuke(AllMachines, "java", 60)

def dstDir=String.format("%s/%s-mode-%s-dbsize-%d", OutputDir, WorkloadName, ModeName, DbSize)
// add suffixes on demand, based on per-experiment variable
//        + String.format("DC-%s-SU-%s-pruning-%d-notifications-%d-SC-%s-TH-%s-records-%d-operations-%d", Topology.datacenters.size(), Topology.datacenters[0].surrogates.size(), PruningIntervalMillis, NotificationsPeriodMillis, Topology.totalScouts(), Threads, DbSize, OpsNum)

pslurp( Scouts, "scout-stdout.txt", dstDir, "scout-stdout.log", 300)
pslurp( Scouts, "scout-stderr.txt", dstDir, "scout-stderr.log", 300)
Topology.datacenters.each { dc ->
    pslurp( dc.surrogates, "sur-stderr.txt", dstDir, "sur-stderr.log", 30)
    pslurp( dc.surrogates, "sur-stdout.txt", dstDir, "sur-stdout.log", 300)
    if (!INTEGRATED_DC) {
        pslurp( dc.sequencers, "seq-stderr.txt", dstDir, "seq-stderr.log", 30)
        pslurp( dc.sequencers, "seq-stdout.txt", dstDir, "seq-stdout.log", 30)
    }
}
configFile = new File(dstDir, "config")
configFile.createNewFile()
configFile.withWriter { out ->
    out.writeLine(config)
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


System.exit(0)

