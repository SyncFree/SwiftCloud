#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib

import static Tools.*
import static PlanetLab_3X.*


def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})


Surrogates = [
    'ec2-54-228-122-250.eu-west-1.compute.amazonaws.com',
    'ec2-54-244-165-15.us-west-2.compute.amazonaws.com',
    'ec2-54-234-203-38.compute-1.amazonaws.com'
]


//    Scouts = (PlanetLab_EU).unique()
Scouts = (PlanetLab_NC + PlanetLab_NV + PlanetLab_EU).unique()

Shepard = Surrogates.get(0);

Threads = 1
Duration = 240
SwiftSocial_Props = "swiftsocial-test.props"


AllMachines = (Surrogates + Scouts + Shepard).unique()

println getBinding().getVariables()

dumpTo(AllMachines, "/tmp/nodes.txt")


pnuke(AllMachines, "java", 60)

//System.exit(0)

println "==== BUILDING JAR..."
sh("ant -buildfile smd-jar-build.xml").waitFor()
deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, SwiftSocial_Props)
//        deployTo(AllMachines, "stuff/all_logging.properties", "all_logging.properties")


def shep = SwiftSocial.runShepard( Shepard, Duration, "Released" )

SwiftSocial.runEachAsDatacentre(Surrogates, "256m", "3096m")
Sleep(10)

//    println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
//    println "==== INITIALIZING DATABASE ===="
//    def INIT_DB_DC = Surrogates.get(0)
//    def INIT_DB_CLIENT = Surrogates.get(0)
//
//    SwiftSocial.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftSocial_Props)


println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
SwiftSocial.runStandaloneScouts( Scouts, Surrogates, SwiftSocial_Props, Shepard, Threads )

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Remaining: ", Duration + 30)

pnuke(AllMachines, "java", 60)

def dstDir="results/swiftsocial/multi_cdf/" + new Date().format('MMMdd-') + System.currentTimeMillis()
def dstFile = String.format("1pc-results-swiftsocial-DC-%s-SC-%s-TH-%s.log", Surrogates.size(), Scouts.size(), Threads)

pslurp( Scouts, "scout-stdout.txt", dstDir, dstFile, 300)

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()

System.exit(0)

