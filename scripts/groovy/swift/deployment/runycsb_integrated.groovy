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

PlanetLab = [
    'planetlab1.xeno.cl.cam.ac.uk',
    'planetlab2.xeno.cl.cam.ac.uk',
    'planetlab-3.imperial.ac.uk',
    'planetlab-4.imperial.ac.uk',
    'planetlab3.xeno.cl.cam.ac.uk',
]


Threads = Integer.valueOf(args[0])

DC_1 = DC([PlanetLab[0]], [PlanetLab[0]])
DC_2 = DC([PlanetLab[2]], [PlanetLab[2]])

Scouts1 = SGroup( PlanetLab[1..1], DC_1 )
Scouts2 = SGroup( PlanetLab[3..3], DC_2 )


Scouts = ( Topology.scouts() ).unique()
ShepardAddr = 'peeramidion.irisa.fr'

AllMachines = ( Topology.allMachines() + ShepardAddr).unique()


// OPTIONS
DbSize = 100000
OpsNum = 100000
PruningIntervalMillis = 30000
NotificationsPeriodMillis = 1000

IncomingOpPerSecLimit = 10000000 // :-)
IncomingOpPerSecPerClientLimit = (int) (IncomingOpPerSecLimit / Scouts.size())

Duration = 360
DurationShepardGrace = 6
InterCmdDelay = 10

WORKLOAD = SwiftYCSB.WORKLOAD_A + ['recordcount': DbSize.toString(), 'operationcount':OpsNum.toString(),
    'target':IncomingOpPerSecPerClientLimit,
    'readproportion':'0.5', 'updateproportion':'0.5','fieldlength':'1',
    'localrequestdistribution':'uniform',
    'localrecordcount':'150',
    'localrequestproportion':'0.8',
]
REPORTS = ['swift.reports':'APP_OP,METADATA', 'swift.reportEveryOperation':'true']
OPTIONS = SwiftBase.CACHING_NOTIFICATIONS_PROPS
YCSB_PROPS = SwiftYCSB.DEFAULT_PROPS + SwiftYCSB.WORKLOAD_A + WORKLOAD + REPORTS + OPTIONS + ['maxexecutiontime' : Duration]

// Options for DB initialization
INIT_NO_REPORTS = ['swift.reports':'']
INIT_OPTIONS = SwiftBase.NO_CACHING_NOTIFICATIONS_PROPS
INIT_THREADS = 2

INIT_YCSB_PROPS = SwiftYCSB.DEFAULT_PROPS + SwiftYCSB.WORKLOAD_A + WORKLOAD + INIT_NO_REPORTS+ INIT_OPTIONS

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

//println "==== LAUNCHING SEQUENCERS"
//Topology.datacenters.each { datacenter ->
//    datacenter.deploySequencers(ShepardAddr, "1024m" )
//}

Sleep(10)
println "==== LAUNCHING SURROGATES"
Topology.datacenters.each { datacenter ->
    datacenter.deployIntegratedSurrogatesExtraArgs(ShepardAddr, "-pruningMs " + PruningIntervalMillis + " -notificationsMs " + NotificationsPeriodMillis, "1536m")
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

def dstDir="results/ycsb/workloada/" + new Date().format('MMMdd-') + System.currentTimeMillis() + "-" + Version + "-" +
        String.format("DC-%s-SU-%s-pruning-%d-notifications-%d-SC-%s-TH-%s-records-%d-operations-%d", Topology.datacenters.size(), Topology.datacenters[0].surrogates.size(), PruningIntervalMillis, NotificationsPeriodMillis, Topology.totalScouts(), Threads, DbSize, OpsNum)

pslurp( Scouts, "scout-stdout.txt", dstDir, "scout-stdout.log", 300)
pslurp( Scouts, "scout-stderr.txt", dstDir, "scout-stderr.log", 300)
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

System.exit(0)

