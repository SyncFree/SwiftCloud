#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib
package swift.deployment

import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})

//West = DC([ "pllx1.parc.xerox.com"], ["pl-node-0.csl.sri.com", "pl-node-1.csl.sri.com"]);
//East = DC([ "planetlab1.cnds.jhu.edu"], ["planetlab2.cnds.jhu.edu"]);
//Europe = DC([ "planetlab-4.imperial.ac.uk"], ["planetlab-3.imperial.ac.uk"]);


Texas = DC([ "ricepl-1.cs.rice.edu"], ["ricepl-2.cs.rice.edu", "ricepl-4.cs.rice.edu", "ricepl-5.cs.rice.edu"]);
East = DC([ "planetlab1.cnds.jhu.edu"], ["planetlab2.cnds.jhu.edu","planetlab3.cnds.jhu.edu", "planetlab4.cnds.jhu.edu"]);
//Europe = DC([ "planetlab-2.imperial.ac.uk"], ["planetlab-1.imperial.ac.uk", "planetlab-4.imperial.ac.uk"]);
 
 
//PT_Clients = SGroup( ["planetlab1.di.fct.unl.pt", "planetlab2.di.fct.unl.pt"], Europe ) 

NV_Clients = SGroup( ["planetlab4.rutgers.edu", "planetlab3.rutgers.edu"], East)

CA_Clients = SGroup( ["planetlab01.cs.washington.edu", "planetlab02.cs.washington.edu"], Texas)

Scouts = ( Topology.scouts() ).unique()

ShepardAddr = "peeramide.irisa.fr"

def Threads = 3
def Duration = 60
def SwiftSocial_Props = "swiftsocial-test.props"

AllMachines = ( Topology.allMachines() + ShepardAddr).unique()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)

println "==== BUILDING JAR..."

sh("ant -buildfile smd-jar-build.xml").waitFor()

deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, "stuff/all_logging.properties", "logging.properties")
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


SwiftSocial2.runScouts( Topology.scoutGroups, SwiftSocial_Props, ShepardAddr, Threads )

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
