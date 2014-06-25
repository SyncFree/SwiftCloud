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
    'ec2-54-76-97-88.eu-west-1.compute.amazonaws.com',
    'ec2-54-76-97-22.eu-west-1.compute.amazonaws.com',
    'ec2-54-76-97-90.eu-west-1.compute.amazonaws.com',
    'ec2-54-76-97-43.eu-west-1.compute.amazonaws.com',
    'ec2-54-76-97-87.eu-west-1.compute.amazonaws.com',
    'ec2-54-76-97-89.eu-west-1.compute.amazonaws.com'
]

NorthVirginiaEC2 = [
    'ec2-54-208-254-68.compute-1.amazonaws.com',
    'ec2-54-209-2-181.compute-1.amazonaws.com',
    'ec2-54-208-250-210.compute-1.amazonaws.com',
    'ec2-54-208-211-241.compute-1.amazonaws.com',
    'ec2-54-208-254-152.compute-1.amazonaws.com',
    'ec2-54-208-243-237.compute-1.amazonaws.com'
]

OregonEC2 = [
    'ec2-54-187-133-163.us-west-2.compute.amazonaws.com',
    'ec2-54-200-2-201.us-west-2.compute.amazonaws.com',
    'ec2-54-187-216-42.us-west-2.compute.amazonaws.com',
    'ec2-54-187-249-241.us-west-2.compute.amazonaws.com',
    'ec2-54-200-27-94.us-west-2.compute.amazonaws.com',
	'ec2-54-186-190-139.us-west-2.compute.amazonaws.com',
]

if (args.length < 2) {
    System.err.println "usage: runsocial.groovy <limits on scouts number per DC> <threads per scout>"
    System.exit(1)
}
PerDCClientNodesLimit = Integer.valueOf(args[0])
Threads = Integer.valueOf(args[1])

Europe = DC([EuropeEC2[0]], [EuropeEC2[0], EuropeEC2[0]])
NorthVirginia = DC([NorthVirginiaEC2[0]], [NorthVirginiaEC2[0], NorthVirginiaEC2[0]])
Oregon = DC([OregonEC2[0]], [OregonEC2[0], OregonEC2[0]])

ScoutsEU = SGroup( EuropeEC2[1..PerDCClientNodesLimit], NorthVirginia )
ScoutsNorthVirginia = SGroup(NorthVirginiaEC2[1..PerDCClientNodesLimit], Oregon )
ScoutsOregon = SGroup(OregonEC2[1..PerDCClientNodesLimit], Europe )

Scouts = ( Topology.scouts() ).unique()
ShepardAddr = Topology.datacenters[0].surrogates[0];

// Threads = 4
Duration = 180
InterCmdDelay = 25
SwiftSocial_Props = "swiftsocial-test.props"

//DbSize = 200*Scouts.size()*Threads
DbSize = 50000
props = SwiftBase.genPropsFile(['swiftsocial.numUsers':DbSize.toString()], SwiftSocial2.DEFAULT_PROPS)

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
    datacenter.deploySurrogates(ShepardAddr, "2048m")
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
        String.format("DC-%s-SU-%s-SC-%s-TH-%s-USERS-%d", Topology.datacenters.size(), Topology.datacenters[0].surrogates.size(), Topology.totalScouts(), Threads, DbSize)

pslurp( Scouts, "scout-stdout.txt", dstDir, "scout-stdout.log", 300)
pslurp( Scouts, "scout-stderr.txt", dstDir, "scout-stderr.log", 300)
props.renameTo(new File(dstDir, SwiftSocial_Props))

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()

System.exit(0)

