#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib
package swift.deployment

import static swift.deployment.PlanetLab_3X.*
import static swift.deployment.Tools.*

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})

Sequencers = ["planetlab-4.imperial.ac.uk"]

Surrogates = ["planetlab-1.imperial.ac.uk","planetlab-2.imperial.ac.uk", "planetlab-3.imperial.ac.uk"]

PlanetLab_PT = [
    "planetlab1.di.fct.unl.pt",
    "planetlab2.di.fct.unl.pt"
]

Scouts = (PlanetLab_PT).unique()

ShepardAddr = Surrogates.get(0)

def Threads = 5
def Duration = 60
def SwiftSocial_Props = "swiftsocial-test.props"


AllMachines = (Sequencers + Surrogates + Scouts + ShepardAddr).unique()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)

println "==== BUILDING JAR..."

sh("ant -buildfile smd-jar-build.xml").waitFor()


deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, "stuff/all_logging.properties", "all_logging.properties")
deployTo(AllMachines, SwiftSocial_Props)

//System.exit(0)

def shep = SwiftSocial.runShepard( ShepardAddr, Duration, "Released" )

SwiftSocial.runEachAsSequencer(Sequencers, Surrogates, "256m")

SwiftSocial.runEachAsSurrogate(Surrogates, Sequencers[0], "512m")

println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
Sleep(10)


println "==== INITIALIZING DATABASE ===="
def INIT_DB_DC = Surrogates.get(0)
def INIT_DB_CLIENT = Surrogates.get(0)

SwiftSocial.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftSocial_Props)


println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
Sleep(20)


SwiftSocial.runStandaloneScouts( Scouts, Surrogates, SwiftSocial_Props, ShepardAddr, Threads )

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Remaining: ", Duration + 30)

pnuke(AllMachines, "java", 60)

def dstDir="results/swiftsocial/" + new Date().format('MMMdd-') + System.currentTimeMillis()
def dstFile = String.format("1pc-results-swiftsocial-DC-%s-SC-%s-TH-%s.log", Surrogates.size(), Scouts.size(), Threads)

pslurp( Scouts, "scout-stdout.txt", dstDir, dstFile, 300)

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()


System.exit(0)

//pssh -t 120 -i -h nodes.txt "ping -a -q -c 10 ec2-107-20-2-64.compute-1.amazonaws.com" | grep mdev | sed "s/\/ //g" | awk '{print $4}' | sed "s/\// /g" | awk '{ print $2 }' | sort
