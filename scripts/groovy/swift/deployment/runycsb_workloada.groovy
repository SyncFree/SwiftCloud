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
    'ec2-54-76-108-59.eu-west-1.compute.amazonaws.com',
    'ec2-54-76-177-207.eu-west-1.compute.amazonaws.com'
]

NVirginiaEC2 = [
    // first node is a DC
    'ec2-54-84-124-179.compute-1.amazonaws.com',
    'ec2-54-88-178-222.compute-1.amazonaws.com'
]

OregonEC2 = [
    // first node is a DC
    'ec2-54-191-90-224.us-west-2.compute.amazonaws.com',
    'ec2-54-191-83-161.us-west-2.compute.amazonaws.com'
]



if (args.length < 1) {
    System.err.println "usage: runycsb_workloada.groovy <threads per scout> [limit on scouts number per DC]"
    System.exit(1)
}
Threads = Integer.valueOf(args[0])
PerDCClientNodesLimit = args.length >= 2 ? Integer.valueOf(args[1]) : Integer.MAX_VALUE

// TOPOLOGY

Europe = DC([EuropeEC2[0]], [EuropeEC2[0]])
NVirginia= DC([NVirginiaEC2[0]], [NVirginiaEC2[0]])
Oregon = DC([OregonEC2[0]], [OregonEC2[0]])
ScoutsEU = SGroup( EuropeEC2[1..Math.min(PerDCClientNodesLimit, EuropeEC2.size() - 1)], Europe )
ScoutsNV = SGroup( NVirginiaEC2[1..Math.min(PerDCClientNodesLimit, NVirginiaEC2.size() - 1)], NVirginia)
ScoutsOR = SGroup( OregonEC2[1..Math.min(PerDCClientNodesLimit, OregonEC2.size() - 1)],  Oregon )
Scouts = ( Topology.scouts() ).unique()
ShepardAddr = Topology.datacenters[0].surrogates[0];
AllMachines = ( Topology.allMachines() + ShepardAddr).unique()


// OPTIONS
DbSize = 10000 // 100000
OpsNum = 1000000
PruningIntervalMillis = 10000

Duration = 480
InterCmdDelay = 30

WORKLOAD = SwiftYCSB.WORKLOAD_A + ['recordcount': DbSize.toString(), 'operationcount':OpsNum.toString(),
    'readproportion':'0.5', 'updateproportion':'0.5','fieldlength':'1']
REPORTS = ['swift.reports':'APP_OP,METADATA']
OPTIONS = SwiftBase.CACHING_NOTIFICATIONS_PROPS + ['swift.disasterSafe':'false']
YCSB_PROPS = SwiftYCSB.DEFAULT_PROPS + SwiftYCSB.WORKLOAD_A + WORKLOAD + REPORTS + OPTIONS

// Options for DB initialization
INIT_NO_REPORTS = ['swift.reports':'']
INIT_OPTIONS = SwiftBase.NO_CACHING_NOTIFICATIONS_PROPS

INIT_YCSB_PROPS = SwiftYCSB.DEFAULT_PROPS + SwiftYCSB.WORKLOAD_A + WORKLOAD + INIT_NO_REPORTS+ INIT_OPTIONS

Version = getGitCommitId()
println getBinding().getVariables()

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

def shep = SwiftBase.runShepard( ShepardAddr, Duration, "Released" )

println "==== LAUNCHING SEQUENCERS"
Topology.datacenters.each { datacenter ->
    datacenter.deploySequencers(ShepardAddr, "1024m" )
}

Sleep(10)
println "==== LAUNCHING SURROGATES"
Topology.datacenters.each { datacenter ->
    datacenter.deploySurrogatesExtraArgs(ShepardAddr, "-pruningMs " + PruningIntervalMillis, "1536m")
}

println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
Sleep(InterCmdDelay)

println "==== INITIALIZING DATABASE ===="
def INIT_DB_DC = Topology.datacenters[0].surrogates[0]
def INIT_DB_CLIENT = Topology.datacenters[0].sequencers[0]

SwiftYCSB.initDB( INIT_DB_CLIENT, INIT_DB_DC, INITYCSBProps, Threads)

println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
Sleep(InterCmdDelay)

SwiftYCSB.runClients(Topology.scoutGroups, YCSBProps, ShepardAddr, Threads, "1024m")

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Max. remaining time: ", Duration + InterCmdDelay)

pnuke(AllMachines, "java", 60)

def dstDir="results/ycsb/workloada/" + new Date().format('MMMdd-') + System.currentTimeMillis() + "-" + Version + "-" +
        String.format("DC-%s-SU-%s-pruning-%d-SC-%s-TH-%s-records-%d-operations-%d", Topology.datacenters.size(), Topology.datacenters[0].surrogates.size(), PruningIntervalMillis, Topology.totalScouts(), Threads, DbSize, OpsNum)

pslurp( Scouts, "scout-stdout.txt", dstDir, "scout-stdout.log", 300)
pslurp( Scouts, "scout-stderr.txt", dstDir, "scout-stderr.log", 300)

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()

System.exit(0)

