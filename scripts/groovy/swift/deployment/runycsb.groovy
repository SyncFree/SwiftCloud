#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.SwiftYCSB.*
import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})

INTEGRATED_DC = true

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-77-125-192.eu-west-1.compute.amazonaws.com'
]

NVirginiaEC2 = [
    // first node is a DC, followed by two groups of 6 and 7 scouts
    'ec2-54-210-105-159.compute-1.amazonaws.com',
    'ec2-107-23-8-93.compute-1.amazonaws.com',
    'ec2-54-210-180-207.compute-1.amazonaws.com',
    'ec2-54-236-226-56.compute-1.amazonaws.com',
    'ec2-107-23-8-117.compute-1.amazonaws.com',
    'ec2-54-210-232-97.compute-1.amazonaws.com',
    'ec2-54-88-244-46.compute-1.amazonaws.com',
    'ec2-54-88-82-117.compute-1.amazonaws.com',
    'ec2-107-23-7-145.compute-1.amazonaws.com',
    'ec2-107-23-7-176.compute-1.amazonaws.com',
    'ec2-54-84-228-145.compute-1.amazonaws.com',
    'ec2-107-23-8-116.compute-1.amazonaws.com',
    'ec2-107-23-8-110.compute-1.amazonaws.com',
    'ec2-54-210-95-27.compute-1.amazonaws.com'
]

OregonEC2 = [
    // first node is a DC, followed by 7 scouts
    'ec2-54-191-105-195.us-west-2.compute.amazonaws.com',
    'ec2-54-201-30-239.us-west-2.compute.amazonaws.com',
    'ec2-54-191-249-227.us-west-2.compute.amazonaws.com',
    'ec2-54-200-227-7.us-west-2.compute.amazonaws.com',
    'ec2-54-200-161-173.us-west-2.compute.amazonaws.com',
    'ec2-54-200-94-27.us-west-2.compute.amazonaws.com',
    'ec2-54-187-61-206.us-west-2.compute.amazonaws.com',
    'ec2-54-201-18-167.us-west-2.compute.amazonaws.com'
]



if (args.length < 1) {
    System.err.println "usage: runycsb_workloada.groovy <threads per scout> [limit on scouts number per DC]"
    System.exit(1)
}
Threads = Integer.valueOf(args[0])
PerDCClientNodesLimit = args.length >= 2 ? Integer.valueOf(args[1]) : Integer.MAX_VALUE

// TOPOLOGY

// planetlab test (unlikely reproducible performance and issues)
//DC_1 = DC([PlanetLab[0]], [PlanetLab[0]])
//DC_2 = DC([PlanetLab[2]], [PlanetLab[2]])
//DC_3 = DC([PlanetLab[4]], [PlanetLab[4]])

//Scouts1 = SGroup( PlanetLab[1..1], DC_1 )
//Scouts2 = SGroup( PlanetLab[3..3], DC_2 )
//Scouts3 = SGroup( PlanetLab[5..5], DC_3 )

Topology.clear()
Threads = Integer.valueOf(args[0])

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
NotificationsPeriodMillis = 1000

IncomingOpPerSecLimit = 12000 // 1000000  // :-)
IncomingOpPerSecPerClientLimit = (int) (IncomingOpPerSecLimit / Scouts.size())

Duration = 600
DurationShepardGrace = 12
InterCmdDelay = 30

WORKLOAD = SwiftYCSB.WORKLOAD_B + ['recordcount': DbSize.toString(), 'operationcount':OpsNum.toString(),
    'target':IncomingOpPerSecPerClientLimit,
    'requestdistribution':'zipfian',

    'localpoolfromglobaldistribution':'true',
    'localrequestdistribution':'uniform',
    'localrecordcount':'150',
    'localrequestproportion':'0.8',
]
// STALENESS_YCSB_READ,STALENESS_YCSB_WRITE,STALENESS_CALIB
REPORTS = ['swift.reports':'APP_OP,APP_OP_FAILURE,METADATA', 'swift.reportEveryOperation':'true']

DC_PROPS = ['swift.reports':'DATABASE_TABLE_SIZE,IDEMPOTENCE_GUARD_SIZE',
            'swift.notificationsFakePracti' : 'false',
            'swift.notificationsDeltaVectors' : 'false',
            ]
OPTIONS = SwiftBase.CACHING_NOTIFICATIONS_PROPS
YCSB_PROPS = SwiftYCSB.DEFAULT_PROPS + WORKLOAD + REPORTS + OPTIONS + ['maxexecutiontime' : Duration]

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

def dstDir="results/ycsb/" + new Date().format('MMMdd-') + System.currentTimeMillis() + "-" + Version + "-" + "test"
// add suffixes on demand, based on per-experiment variable
//        + String.format("DC-%s-SU-%s-pruning-%d-notifications-%d-SC-%s-TH-%s-records-%d-operations-%d", Topology.datacenters.size(), Topology.datacenters[0].surrogates.size(), PruningIntervalMillis, NotificationsPeriodMillis, Topology.totalScouts(), Threads, DbSize, OpsNum)

pslurp( Scouts, "scout-stdout.txt", dstDir, "scout-stdout.log", 300)
pslurp( Scouts, "scout-stderr.txt", dstDir, "scout-stderr.log", 300)
Topology.datacenters.each { dc ->
    pslurp( dc.surrogates, "sur-stderr.txt", dstDir, "sur-stderr.log", 30)
    pslurp( dc.surrogates, "sur-stdout.txt", dstDir, "sur-stdout.log", 30)
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

