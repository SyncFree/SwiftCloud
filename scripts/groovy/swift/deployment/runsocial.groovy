#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;


def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})


Europe = DC(['ec2-54-76-30-169.eu-west-1.compute.amazonaws.com'], 
					['ec2-54-76-30-169.eu-west-1.compute.amazonaws.com'])

NorthVirginia = DC(['ec2-54-86-30-43.compute-1.amazonaws.com'], 
					['ec2-54-86-30-43.compute-1.amazonaws.com'])

//Oregon = DC(['ec2-54-200-37-248.us-west-2.compute.amazonaws.com'], 
//					['ec2-54-200-37-248.us-west-2.compute.amazonaws.com']


ScoutsEU = SGroup( [
    'ec2-54-76-36-191.eu-west-1.compute.amazonaws.com',
    'ec2-54-76-36-201.eu-west-1.compute.amazonaws.com'
], Europe )


ScoutsNorthVirginia = SGroup( 	[
    'ec2-54-86-140-91.compute-1.amazonaws.com',
    'ec2-54-86-159-33.compute-1.amazonaws.com'
], NorthVirginia )


//ScoutsOregon = SGroup( 	[
//    'ec2-54-200-37-250.us-west-2.compute.amazonaws.com',
//    'ec2-54-200-37-249.us-west-2.compute.amazonaws.com',
//    'ec2-54-200-38-2.us-west-2.compute.amazonaws.com'
//], Oregon )


/*
Texas = DC([ "ricepl-1.cs.rice.edu"], ["ricepl-2.cs.rice.edu", "ricepl-4.cs.rice.edu", "ricepl-5.cs.rice.edu"]);
East = DC([ "planetlab1.cnds.jhu.edu"], ["planetlab2.cnds.jhu.edu","planetlab3.cnds.jhu.edu", "planetlab4.cnds.jhu.edu"]);

NV_Clients = SGroup( ["planetlab4.rutgers.edu", "planetlab3.rutgers.edu"], East)
CA_Clients = SGroup( ["planetlab01.cs.washington.edu", "planetlab02.cs.washington.edu"], Texas)
*/

Scouts = ( Topology.scouts() ).unique()
ShepardAddr = Topology.datacenters[0].surrogates[0];

Threads = 4
Duration = 240
InterCmdDelay = 30
SwiftSocial_Props = "swiftsocial-test.props"

AllMachines = ( Topology.allMachines() + ShepardAddr).unique()

Version = getGitCommitId()

println getBinding().getVariables()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)

//System.exit(0)

println "==== BUILDING JAR for version " + Version + "..."
sh("ant -buildfile smd-jar-build.xml").waitFor()
deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, SwiftSocial_Props)
//        deployTo(AllMachines, "stuff/all_logging.properties", "logging.properties")


def shep = SwiftSocial.runShepard( ShepardAddr, Duration, "Released" )

println "==== LAUNCHING SEQUENCERS"
Topology.datacenters.each { datacenter ->
	datacenter.deploySequencers(ShepardAddr ) 
}

Sleep(10)
println "==== LAUNCHING SURROGATES"
Topology.datacenters.each { datacenter ->
	datacenter.deploySurrogates(ShepardAddr, "512m") 
}

println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
Sleep(InterCmdDelay)

println "==== INITIALIZING DATABASE ===="
def INIT_DB_DC = Topology.datacenters[0].surrogates[0]
def INIT_DB_CLIENT = Topology.datacenters[0].sequencers[0]

SwiftSocial2.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftSocial_Props)

println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
Sleep(InterCmdDelay)

SwiftSocial2.runScouts( Topology.scoutGroups, SwiftSocial_Props, ShepardAddr, Threads )

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Max. remaining time: ", Duration + InterCmdDelay)

pnuke(AllMachines, "java", 60)

def dstDir="results/swiftsocial/" + new Date().format('MMMdd-') +
        System.currentTimeMillis() + "-" + Version + "-" +
        String.format("DC-%s-SU-%s-SC-%s-TH-%s", Topology.datacenters.size(), Topology.datacenters[0].surrogates.size(), Topology.totalScouts(), Threads)

pslurp( Scouts, "scout-stdout.txt", dstDir, "scout-stdout.log", 300)

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()

System.exit(0)

