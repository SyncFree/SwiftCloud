#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib

package swift.deployment

import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})

def PEERS = ['ricepl-1.cs.rice.edu', 
			'planetlab1.cnds.jhu.edu', 
			'planetlab-4.imperial.ac.uk', 
			'planetlab4.rutgers.edu', 
			'peeramide.irisa.fr',
			'host2.planetlab.informatik.tu-darmstadt.de',
			'planet1.servers.ua.pt',
			'planetlab-1.research.netlab.hut.fi',
			'planetlab1.unineuchatel.ch',
			'pl-node-0.csl.sri.com',
			'planetlab1.cs.colorado.edu',
			'planetlab1.cs.umass.edu'
			]

//Creates a Datacenter at each peer, running a sequencer and a surrogate and, finally, a swiftsocial client at each site
PEERS.each {
		dc = DC([it],[it])
		SGroup([it], dc)
}

Scouts = ( Topology.scouts() ).unique()

ShepardAddr = "peeramide.irisa.fr"

def Threads = 1
def Duration = 60
def SwiftSocial_Props = "swiftsocial-test-p2p.props"

AllMachines = ( Topology.allMachines() + ShepardAddr).unique()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)

println "==== BUILDING JAR..."

sh("ant -buildfile smd-jar-build.xml").waitFor()

deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, "stuff/logging.properties", "logging.properties")
deployTo(AllMachines, SwiftSocial_Props)

def shep = SwiftSocial.runShepard( ShepardAddr, Duration, "Released" )

println "==== LAUNCHING SEQUENCERS"
Topology.datacenters.each { datacenter ->
	datacenter.deploySequencers(ShepardAddr ) 
}
Sleep(10)
println "==== LAUNCHING SURROGATES"
Topology.datacenters.each { datacenter ->
	datacenter.deploySurrogates(ShepardAddr) 
}


println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
Sleep(15)

println "==== INITIALIZING DATABASE ===="
def INIT_DB_DC = Topology.datacenters[0].surrogates[0]
def INIT_DB_CLIENT = Topology.datacenters[0].sequencers[0]

SwiftSocial2.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftSocial_Props)

println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
Sleep(20)


SwiftSocial2.runScouts( Topology.scoutGroups, SwiftSocial_Props, ShepardAddr, Threads,"256m" )

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Remaining: ", Duration + 30)

pnuke(Scouts, "java", 60)
pnuke(AllMachines, "java", 60)

def dstDir="results/swiftsocial/" + new Date().format('MMMdd-') + System.currentTimeMillis()
def dstFile = String.format("1pc-results-swiftsocial-DC-%s-SU-%s-CL-%s-TH-%s.log", Topology.datacenters.size(), Topology.datacenters[0].surrogates.size(), Topology.totalScouts(), Threads)

pslurp( Scouts, "scout-stdout.txt", dstDir, dstFile, 300)

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()


System.exit(0)

//pssh -t 120 -i -h nodes.txt "ping -a -q -c 10 ec2-107-20-2-64.compute-1.amazonaws.com" | grep mdev | sed "s/\/ //g" | awk '{print $4}' | sed "s/\// /g" | awk '{ print $2 }' | sort
