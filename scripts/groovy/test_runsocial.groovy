#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib

import static Tools.*
import static PlanetLab_3X.*

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})

Surrogates = [
        "planet1.zib.de",
        ]

PlanetLab_PT = [
   "planetlab1.fct.ualg.pt",
   "planetlab2.fct.ualg.pt",
   "planetlab1.di.fct.unl.pt",
   "planetlab2.di.fct.unl.pt"
]

Scouts = (PlanetLab_PT).unique()
//Scouts = (PlanetLab_ASIA_RAW).unique()

Shepard = Surrogates.get(0)

def Threads = 1
def Duration = 60
def SwiftSocial_Props = "swiftsocial-test.props"


AllMachines = (Surrogates + Scouts + Shepard).unique()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)
//System.exit(0)

println "==== BUILDING JAR..."

sh("ant -buildfile smd-jar-build.xml").waitFor()


deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, "stuff/all_logging.properties", "all_logging.properties")
deployTo(AllMachines, SwiftSocial_Props)

//System.exit(0)

def shep = SwiftSocial.runShepard( Shepard, Duration, "Released" )

SwiftSocial.runEachAsDatacentre(Surrogates, "256m", "512m")

println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
Sleep(10)


println "==== INITIALIZING DATABASE ===="
def INIT_DB_DC = Surrogates.get(0)
def INIT_DB_CLIENT = Surrogates.get(0)

SwiftSocial.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftSocial_Props)


println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
Sleep(10)


SwiftSocial.runStandaloneScouts( Scouts, Surrogates, SwiftSocial_Props, Shepard, Threads )

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Remaining: ", Duration + 30)

//pnuke(AllMachines, "java", 60)

def dstDir="results/swiftsocial/" + new Date().format('MMMdd-') + System.currentTimeMillis()
def dstFile = String.format("1pc-results-swiftsocial-DC-%s-SC-%s-TH-%s.log", Surrogates.size(), Scouts.size(), Threads)

pslurp( Scouts, "scout-stdout.txt", dstDir, dstFile, 300)

exec(["/bin/bash", "-c", "wc " + dstDir + "/*/*"]).waitFor()
System.exit(0)

//pssh -t 120 -i -h nodes.txt "ping -a -q -c 10 ec2-107-20-2-64.compute-1.amazonaws.com" | grep mdev | sed "s/\/ //g" | awk '{print $4}' | sed "s/\// /g" | awk '{ print $2 }' | sort
