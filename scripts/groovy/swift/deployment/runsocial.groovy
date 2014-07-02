#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;


def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})


EuropeEC2 = [
    'ec2-54-77-18-173.eu-west-1.compute.amazonaws.com',
    'ec2-54-77-7-59.eu-west-1.compute.amazonaws.com',
]

NorthVirginiaEC2 = []

OregonEC2 = []

if (args.length < 1) {
    System.err.println "usage: runsocial.groovy <threads per scout> [limit on scouts number per DC]"
    System.exit(1)
}
Threads = Integer.valueOf(args[0])
PerDCClientNodesLimit = args.length >= 2 ? Integer.valueOf(args[1]) : Integer.MAX_VALUE

Europe = DC([EuropeEC2[0]], [EuropeEC2[0]])
//NVirginia= DC([NVirginiaEC2[0]], [NVirginiaEC2[0]])
//Oregon = DC([OregonEC2[0]], [OregonEC2[0]])
ScoutsEU = SGroup( EuropeEC2[1..Math.min(PerDCClientNodesLimit, EuropeEC2.size() - 1)], Europe )
//ScoutsNV = SGroup( NVirginiaEC2[1..Math.min(PerDCClientNodesLimit, NVirginiaEC2.size() - 1)], NVirginia)
//ScoutsOR = SGroup( OregonEC2[1..Math.min(PerDCClientNodesLimit, OregonEC2.size() - 1)],  Oregon )

Scouts = ( Topology.scouts() ).unique()
ShepardAddr = Topology.datacenters[0].surrogates[0];

// Threads = 4
Duration = 240
InterCmdDelay = 25

PruningIntervalMillis = 5000
//DbSize = 200*Scouts.size()*Threads
DbSize = 50000
SwiftSocial_Props = "swiftsocial-test.props"
props = SwiftBase.genPropsFile(['swiftsocial.numUsers':DbSize.toString(),
    'swift.reports':'APP_OP,METADATA'], SwiftSocial2.DEFAULT_PROPS)

AllMachines = ( Topology.allMachines() + ShepardAddr).unique()

Version = getGitCommitId()

println getBinding().getVariables()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)

//System.exit(0)

println "==== BUILDING JAR for version " + Version + "..."
sh("ant -buildfile smd-jar-build.xml").waitFor()
deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, props.absolutePath, SwiftSocial_Props)
deployTo(AllMachines, "stuff/logging.properties", "logging.properties")


def shep = SwiftBase.runShepard( ShepardAddr, Duration, "Released" )

println "==== LAUNCHING SEQUENCERS"
Topology.datacenters.each { datacenter ->
    datacenter.deploySequencers(ShepardAddr, "1024m")
}

Sleep(10)
println "==== LAUNCHING SURROGATES"
Topology.datacenters.each { datacenter ->
    datacenter.deploySurrogatesExtraArgs(ShepardAddr, "-pruningMs " + PruningIntervalMillis, "2048m")
}

println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
Sleep(InterCmdDelay)

println "==== INITIALIZING DATABASE ===="
def INIT_DB_DC = Topology.datacenters[0].surrogates[0]
def INIT_DB_CLIENT = Topology.datacenters[0].sequencers[0]

SwiftSocial2.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftSocial_Props, "1024m")

println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
Sleep(InterCmdDelay)

SwiftSocial2.runScouts( Topology.scoutGroups, SwiftSocial_Props, ShepardAddr, Threads, "3648m" )

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Max. remaining time: ", Duration + InterCmdDelay)

pnuke(AllMachines, "java", 60)

def dstDir="results/staleness/swiftsocial/" + new Date().format('MMMdd-') +
        System.currentTimeMillis() + "-" + Version + "-" +
        String.format("DC-%s-SU-%s-pruning-%d-SC-%s-TH-%s-USERS-%d", Topology.datacenters.size(), Topology.datacenters[0].surrogates.size(), PruningIntervalMillis, Topology.totalScouts(), Threads, DbSize)

pslurp( Scouts, "scout-stdout.txt", dstDir, "scout-stdout.log", 300)
pslurp( Scouts, "scout-stderr.txt", dstDir, "scout-stderr.log", 300)
props.renameTo(new File(dstDir, SwiftSocial_Props))

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()

System.exit(0)

