#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib

import static Tools.*

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})

Surrogates = [
    "peeramide.irisa.fr",
]

PlanetLab_PT = [
    "planetlab1.di.fct.unl.pt",
    "planetlab2.di.fct.unl.pt",
    "planetlab1.fct.ualg.pt",
    "planetlab2.fct.ualg.pt",
]

Scouts = (PlanetLab_PT).unique()

Shepard = "planetlab-2.tagus.ist.utl.pt"

def Threads = 3
def Duration = 300
def SwiftSocial_Props = "swiftsocial-test.props"


AllMachines = (Surrogates + Scouts + Shepard).unique()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)


println "==== BUILDING JAR..."

sh("ant -buildfile smd-jar-build.xml").waitFor()


deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, "stuff/all_logging.properties", "all_logging.properties")
deployTo(AllMachines, SwiftSocial_Props)


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

pnuke(AllMachines, "java", 60)

def dstDir="results/swiftsocial/" + new Date().format('MMMdd-') + System.currentTimeMillis()
def dstFile = String.format("1pc-results-swiftsocial-DC-%s-SC-%s-TH-%s.log", Surrogates.size(), Scouts.size(), Threads)

pslurp( Scouts, "scout-stdout.txt", dstDir, dstFile, 300)

exec(["/bin/bash", "-c", "wc " + dstDir + "/*/*"]).waitFor()
System.exit(0)

