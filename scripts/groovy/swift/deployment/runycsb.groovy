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

// NODES

EuropeEC2 = [
    // first node is a DC
]

NVirginiaEC2 = [
    // first node is a DC
]

OregonEC2 = [
    // first node is a DC
]



if (args.length < 1) {
    System.err.println "usage: runycsb_workloada.groovy <threads per scout> [limit on scouts number per DC]"
    System.exit(1)
}
Threads = Integer.valueOf(args[0])
PerDCClientNodesLimit = args.length >= 2 ? Integer.valueOf(args[1]) : Integer.MAX_VALUE

// TOPOLOGY

Topology.clear()

// Europe = DC([EuropeEC2[0]], [EuropeEC2[0]])
// NVirginia= DC([NVirginiaEC2[0]], [NVirginiaEC2[0]])
Oregon =  DC([
    'ec2-54-187-160-245.us-west-2.compute.amazonaws.com'
], [
    'ec2-54-187-160-245.us-west-2.compute.amazonaws.com'
]) //DC([OregonEC2[0]], [OregonEC2[0]])
//ScoutsEU = SGroup( EuropeEC2[1..Math.min(PerDCClientNodesLimit, EuropeEC2.size() - 1)], Europe )
ScoutsNV = SGroup([
    'ec2-54-210-30-38.compute-1.amazonaws.com',
    'ec2-54-86-101-104.compute-1.amazonaws.com'
], Oregon)
//SGroup( NVirginiaEC2[1..Math.min(PerDCClientNodesLimit, NVirginiaEC2.size() - 1)], NVirginia)
//ScoutsOR = SGroup( OregonEC2[1..Math.min(PerDCClientNodesLimit, OregonEC2.size() - 1)],  Oregon )
Scouts = ( Topology.scouts() ).unique()
ShepardAddr = Topology.datacenters[0].surrogates[0];
AllMachines = ( Topology.allMachines() + ShepardAddr).unique()


// OPTIONS
DbSize = 100000
OpsNum = 1000000
PruningIntervalMillis = 30000
NotificationsPeriodMillis = 1000

IncomingOpPerSecLimit = 10000000 // :-)
IncomingOpPerSecPerClientLimit = (int) (IncomingOpPerSecLimit / Scouts.size())

Duration = 360
DurationShepardGrace = 12
InterCmdDelay = 30

WORKLOAD = SwiftYCSB.WORKLOAD_A + ['recordcount': DbSize.toString(), 'operationcount':OpsNum.toString(),
    'target':IncomingOpPerSecPerClientLimit,
    'requestdistribution':'zipfian',

    'localpoolfromglobaldistribution':'true',
    'localrequestdistribution':'uniform',
    'localrecordcount':'150',
    'localrequestproportion':'0.8',
]
REPORTS = ['swift.reports':'APP_OP,APP_OP_FAILURE,METADATA,STALENESS_YCSB_READ,STALENESS_YCSB_WRITE,STALENESS_CALIB', 'swift.reportEveryOperation':'true']
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

println "==== LAUNCHING SEQUENCERS"
Topology.datacenters.each { datacenter ->
    datacenter.deploySequencers(ShepardAddr, "1024m" )
}

Sleep(10)
println "==== LAUNCHING SURROGATES"
Topology.datacenters.each { datacenter ->
    datacenter.deploySurrogatesExtraArgs(ShepardAddr, "-pruningMs " + PruningIntervalMillis + " -notificationsMs " + NotificationsPeriodMillis, "1536m")
}

println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
Sleep(InterCmdDelay)

println "==== INITIALIZING DATABASE ===="
def INIT_DB_DC = Topology.datacenters[0].surrogates[0]
def INIT_DB_CLIENT = Topology.datacenters[0].sequencers[0]

SwiftYCSB.initDB( INIT_DB_CLIENT, INIT_DB_DC, INITYCSBProps, INIT_THREADS)

println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
Sleep(InterCmdDelay)

SwiftYCSB.runClients(Topology.scoutGroups, YCSBProps, ShepardAddr, Threads, "1024m")

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
    pslurp( dc.sequencers, "seq-stderr.txt", dstDir, "seq-stderr.log", 30)
    pslurp( dc.sequencers, "seq-stdout.txt", dstDir, "seq-stdout.log", 30)
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


System.exit(0)

