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


EuropeEC2 = [
    'ec2-54-76-253-182.eu-west-1.compute.amazonaws.com',
    'ec2-54-77-12-60.eu-west-1.compute.amazonaws.com',
    'ec2-54-77-12-63.eu-west-1.compute.amazonaws.com',
    'ec2-54-77-12-7.eu-west-1.compute.amazonaws.com',
    'ec2-54-77-12-61.eu-west-1.compute.amazonaws.com'
]

NVirginiaEC2 = [
    'ec2-54-86-89-171.compute-1.amazonaws.com',
    'ec2-54-88-141-139.compute-1.amazonaws.com',
    'ec2-54-88-191-167.compute-1.amazonaws.com',
    'ec2-54-88-160-165.compute-1.amazonaws.com',
    'ec2-54-88-187-239.compute-1.amazonaws.com'
]

OregonEC2 = [
    'ec2-54-76-217-44.eu-west-1.compute.amazonaws.com',
    // 'ec2-54-191-31-66.us-west-2.compute.amazonaws.com',
    'ec2-54-191-49-168.us-west-2.compute.amazonaws.com',
    'ec2-54-187-29-100.us-west-2.compute.amazonaws.com',
    'ec2-54-191-54-93.us-west-2.compute.amazonaws.com',
    'ec2-54-191-54-108.us-west-2.compute.amazonaws.com'
]



if (args.length < 2) {
    System.err.println "usage: runycsb_workloada.groovy <limits on scouts number per DC> <threads per scout>"
    System.exit(1)
}
PerDCClientNodesLimit = Integer.valueOf(args[0])
Threads = Integer.valueOf(args[1])


Europe = DC([EuropeEC2[0]], [EuropeEC2[0]])
//NVirginia= DC([NVirginiaEC2[0]], [NVirginiaEC2[0]])
//Oregon = DC([OregonEC2[0]], [OregonEC2[0]])
ScoutsEU = SGroup( EuropeEC2[1..PerDCClientNodesLimit], Europe )
//ScoutsNV = SGroup( NVirginiaEC2[1..PerDCClientNodesLimit], NVirginia)
//ScoutsOR = SGroup( OregonEC2[1..PerDCClientNodesLimit],  Oregon )
Scouts = ( Topology.scouts() ).unique()
ShepardAddr = Topology.datacenters[0].surrogates[0];
AllMachines = ( Topology.allMachines() + ShepardAddr).unique()

YCSBProps = "swiftycsb.properties"
DbSize = 100000
OpsNum = 1000000

Duration = 240
InterCmdDelay = 30

Version = getGitCommitId()
println getBinding().getVariables()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)


println "==== BUILDING JAR for version " + Version + "..."
sh("ant -buildfile smd-jar-build.xml").waitFor()
deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, "stuff/logging.properties", "logging.properties")
deployTo(AllMachines, SwiftYCSB.genPropsFile(['recordcount': DbSize.toString(),
    'operationcount':OpsNum.toString(), 'swift.reportEveryOperation':'true', 'readproportion':'0.5',
    'updateproportion':'0.5','fieldlength':'1',
    //    'swift.reports':'APP_OP,METADATA',
    //    'swift.cacheSize':'256',
    //    'swift.asyncCommit':'true',
    //    'swift.notifications':'true',
    //    'swift.causalNotifications':'true'
], SwiftYCSB.DEFAULT_PROPS + SwiftYCSB.WORKLOAD_A).absolutePath, YCSBProps)

def shep = SwiftBase.runShepard( ShepardAddr, Duration, "Released" )

println "==== LAUNCHING SEQUENCERS"
Topology.datacenters.each { datacenter ->
    datacenter.deploySequencers(ShepardAddr, "1024m" )
}

Sleep(10)
println "==== LAUNCHING SURROGATES"
Topology.datacenters.each { datacenter ->
    datacenter.deploySurrogates(ShepardAddr, "1536m")
}

println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
Sleep(InterCmdDelay)

println "==== INITIALIZING DATABASE ===="
def INIT_DB_DC = Topology.datacenters[0].surrogates[0]
def INIT_DB_CLIENT = Topology.datacenters[0].sequencers[0]

SwiftYCSB.initDB( INIT_DB_CLIENT, INIT_DB_DC, YCSBProps, Threads)

println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
Sleep(InterCmdDelay)

SwiftYCSB.runClients(Topology.scoutGroups, YCSBProps, ShepardAddr, Threads, "1024m")

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Max. remaining time: ", Duration + InterCmdDelay)

pnuke(AllMachines, "java", 60)

def dstDir="results/ycsb/workloada/" + new Date().format('MMMdd-') + System.currentTimeMillis() + "-" + Version + "-" +
        String.format("DC-%s-SU-%s-SC-%s-TH-%s-records-%d-operations-%d", Topology.datacenters.size(), Topology.datacenters[0].surrogates.size(), Topology.totalScouts(), Threads, DbSize, OpsNum)

pslurp( Scouts, "scout-stdout.txt", dstDir, "scout-stdout.log", 300)
pslurp( Scouts, "scout-stderr.txt", dstDir, "scout-stderr.log", 300)

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()

System.exit(0)

