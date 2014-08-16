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

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-76-188-118.eu-west-1.compute.amazonaws.com'
]

NVirginiaEC2 = [
    // first node is a DC, followed by two groups of 6 and 7 scouts
    'ec2-107-23-34-74.compute-1.amazonaws.com',
    'ec2-54-210-190-244.compute-1.amazonaws.com',
    'ec2-107-23-34-114.compute-1.amazonaws.com',
    'ec2-107-23-35-152.compute-1.amazonaws.com',
    'ec2-107-23-34-194.compute-1.amazonaws.com',
    'ec2-54-210-189-179.compute-1.amazonaws.com',
    'ec2-107-21-6-172.compute-1.amazonaws.com',
    'ec2-107-23-35-19.compute-1.amazonaws.com',
    'ec2-107-23-4-205.compute-1.amazonaws.com',
    'ec2-107-23-42-21.compute-1.amazonaws.com',
    'ec2-54-210-190-61.compute-1.amazonaws.com',
    'ec2-107-23-42-113.compute-1.amazonaws.com',
    'ec2-54-236-249-84.compute-1.amazonaws.com',
    'ec2-54-210-248-126.compute-1.amazonaws.com'
]

OregonEC2 = [
    // first node is a DC, followed by 7 scouts
    'ec2-54-201-151-156.us-west-2.compute.amazonaws.com',
    'ec2-54-201-151-159.us-west-2.compute.amazonaws.com',
    'ec2-54-201-151-204.us-west-2.compute.amazonaws.com',
    'ec2-54-201-151-196.us-west-2.compute.amazonaws.com',
    'ec2-54-201-151-221.us-west-2.compute.amazonaws.com',
    'ec2-54-201-151-220.us-west-2.compute.amazonaws.com',
    'ec2-54-201-151-166.us-west-2.compute.amazonaws.com',
    'ec2-54-201-151-173.us-west-2.compute.amazonaws.com'
]



// TODO: avoid copy-pasting and redundancy with runycsb.groovy

Proportion = "0.8"
Threads = 40

if (args.length != 3) {
    System.err.println "usage: scalabilitythroughput.groovy <workload> <mode> <opslimit> "
    System.exit(1)
}
WorkloadName = args[0]
ModeName = args[1]
IncomingOpPerSecLimit  = Integer.parseInt(args[2])
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
]
Mode = MODES[ModeName]


// TOPOLOGY

// planetlab test (unlikely reproducible performance and issues)
//DC_1 = DC([PlanetLab[0]], [PlanetLab[0]])
//DC_2 = DC([PlanetLab[2]], [PlanetLab[2]])
//DC_3 = DC([PlanetLab[4]], [PlanetLab[4]])

//Scouts1 = SGroup( PlanetLab[1..1], DC_1 )
//Scouts2 = SGroup( PlanetLab[3..3], DC_2 )
//Scouts3 = SGroup( PlanetLab[5..5], DC_3 )

Topology.clear()

DC_EU = DC([EuropeEC2[0]], [EuropeEC2[0]])
DC_NV = DC([NVirginiaEC2[0]], [NVirginiaEC2[0]])
DC_OR = DC([OregonEC2[0]], [OregonEC2[0]])

ScoutsToEU = SGroup(NVirginiaEC2[1..6], DC_EU)
ScoutsToNV = SGroup(OregonEC2[1..7], DC_NV)
ScoutsToOR = SGroup(NVirginiaEC2[7..13], DC_OR)

Scouts = ( Topology.scouts() ).unique()
ShepardAddr = Topology.datacenters[0].surrogates[0];
AllMachines = ( Topology.allMachines() + ShepardAddr).unique()


// OPTIONS
DbSize = 100000
OpsNum = 1000000
PruningIntervalMillis = 60000
NotificationsPeriodMillis = Mode.containsKey('swift.notificationPeriodMillis') ? Mode['swift.notificationPeriodMillis'] : '1000'

IncomingOpPerSecPerClientLimit = (int) (IncomingOpPerSecLimit / Scouts.size())

Duration = 600
DurationShepardGrace = 12
InterCmdDelay = 30

WORKLOAD = BaseWorkload + ['recordcount': DbSize.toString(), 'operationcount':OpsNum.toString(),
    'target':IncomingOpPerSecPerClientLimit,
    // 'requestdistribution':'uniform',

    'localpoolfromglobaldistribution':'true',
    'localrequestdistribution':'uniform',
    'localrecordcount':'150',
    'localrequestproportion':Proportion
]
REPORTS = ['swift.reports':'APP_OP,APP_OP_FAILURE,METADATA', 'swift.reportEveryOperation':'true']
DC_PROPS = ['swift.reports':'DATABASE_TABLE_SIZE,IDEMPOTENCE_GUARD_SIZE']
YCSB_PROPS = SwiftYCSB.DEFAULT_PROPS + WORKLOAD + REPORTS + Mode + ['maxexecutiontime' : Duration]

// Options for DB initialization
INIT_NO_REPORTS = ['swift.reports':'']
INIT_OPTIONS = SwiftBase.NO_CACHING_NOTIFICATIONS_PROPS
INIT_THREADS = 4

INIT_YCSB_PROPS = SwiftYCSB.DEFAULT_PROPS + WORKLOAD + ['target':'10000000'] + INIT_NO_REPORTS+ INIT_OPTIONS

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

println "==== LAUNCHING SEQUENCERS"
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

SwiftYCSB.runClients(Topology.scoutGroups, YCSBProps, ShepardAddr, Threads, "3072m")

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Max. remaining time: ", Duration + InterCmdDelay)

pnuke(AllMachines, "java", 60)

def dstDir="results/ycsb/multi-DC/scalabilitythroughput/" +
        String.format("%s-mode-%s-opslimit-%d", WorkloadName, ModeName, IncomingOpPerSecLimit)

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

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()

def compressor = exec([
    "tar",
    "-czf",
    dstDir+".tar.gz",
    dstDir
])
compressor.waitFor()
exec(["/bin/rm", "-Rf", dstDir]).waitFor()

System.exit(0)

