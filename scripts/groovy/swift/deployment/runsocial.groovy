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
    'ec2-54-76-46-41.eu-west-1.compute.amazonaws.com',
    'ec2-54-76-46-25.eu-west-1.compute.amazonaws.com',
    'ec2-54-72-227-177.eu-west-1.compute.amazonaws.com',
    'ec2-54-76-46-49.eu-west-1.compute.amazonaws.com',
    'ec2-54-72-52-235.eu-west-1.compute.amazonaws.com',
    'ec2-54-72-217-106.eu-west-1.compute.amazonaws.com'
]

NorthVirginiaEC2 = [
    'ec2-54-86-252-163.compute-1.amazonaws.com',
    'ec2-54-86-252-161.compute-1.amazonaws.com',
    'ec2-54-86-124-17.compute-1.amazonaws.com',
    'ec2-54-86-252-209.compute-1.amazonaws.com',
    'ec2-54-86-223-100.compute-1.amazonaws.com',
    'ec2-54-86-197-100.compute-1.amazonaws.com'
]

OregonEC2 = [
    'ec2-54-200-13-249.us-west-2.compute.amazonaws.com',
    'ec2-54-187-230-35.us-west-2.compute.amazonaws.com',
    'ec2-54-187-230-38.us-west-2.compute.amazonaws.com',
    'ec2-54-187-223-234.us-west-2.compute.amazonaws.com',
    'ec2-54-200-29-62.us-west-2.compute.amazonaws.com',
    'ec2-54-200-29-51.us-west-2.compute.amazonaws.com'
]

// Optional argument - limit of scouts number
if (args.length < 2) {
    System.exit(1)
}
PerDCClientNodesLimit = Integer.valueOf(args[0])
Threads = Integer.valueOf(args[1])

//AllEC2 = EuropeEC2 + NorthVirginiaEC2 + OregonEC2
//
//
//AllEC2.each { node ->
//    dc = DC([node], [node])
//    SGroup([node], dc)
//}

Europe = DC([EuropeEC2[0]], [EuropeEC2[0]])
NorthVirginia = DC([NorthVirginiaEC2[0]], [NorthVirginiaEC2[0]])
Oregon = DC([OregonEC2[0]], [OregonEC2[0]])

ScoutsEU = SGroup( EuropeEC2[1..PerDCClientNodesLimit], NorthVirginia )
ScoutsNorthVirginia = SGroup(NorthVirginiaEC2[1..PerDCClientNodesLimit], Oregon )
ScoutsOregon = SGroup(OregonEC2[1..PerDCClientNodesLimit], Europe )

/*
 Texas = DC([ "ricepl-1.cs.rice.edu"], ["ricepl-2.cs.rice.edu", "ricepl-4.cs.rice.edu", "ricepl-5.cs.rice.edu"]);
 East = DC([ "planetlab1.cnds.jhu.edu"], ["planetlab2.cnds.jhu.edu","planetlab3.cnds.jhu.edu", "planetlab4.cnds.jhu.edu"]);
 NV_Clients = SGroup( ["planetlab4.rutgers.edu", "planetlab3.rutgers.edu"], East)
 CA_Clients = SGroup( ["planetlab01.cs.washington.edu", "planetlab02.cs.washington.edu"], Texas)
 */

Scouts = ( Topology.scouts() ).unique()
ShepardAddr = Topology.datacenters[0].surrogates[0];

// Threads = 4
Duration = 240
InterCmdDelay = 30
SwiftSocial_Props = "swiftsocial-test.props"

DbSize = 200*Scouts.size()*Threads
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


def shep = SwiftSocial.runShepard( ShepardAddr, Duration, "Released" )

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

SwiftSocial2.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftSocial_Props, "1024m")

println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
Sleep(InterCmdDelay)

SwiftSocial2.runScouts( Topology.scoutGroups, SwiftSocial_Props, ShepardAddr, Threads, "2048m" )

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Max. remaining time: ", Duration + InterCmdDelay)

pnuke(AllMachines, "java", 60)

def dstDir="results/swiftsocial/" + new Date().format('MMMdd-') +
        System.currentTimeMillis() + "-" + Version + "-" +
        String.format("DC-%s-SU-%s-SC-%s-TH-%s-USERS-%d", Topology.datacenters.size(), Topology.datacenters[0].surrogates.size(), Topology.totalScouts(), Threads, DbSize)

pslurp( Scouts, "scout-stdout.txt", dstDir, "scout-stdout.log", 300)
props.renameTo(new File(dstDir, SwiftSocial_Props))

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()

System.exit(0)

