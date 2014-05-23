#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib
package swift.deployment

import static swift.deployment.Tools.*

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})

Surrogates = [
    "ec2-50-112-194-118.us-west-2.compute.amazonaws.com",
    "ec2-46-137-61-52.eu-west-1.compute.amazonaws.com",
]

PlanetLab = ["pl4.cs.unm.edu"]

Scouts = (PlanetLab).unique()

ShepardAddr = "peeramide.irisa.fr";

Threads = 1
Duration = 80
SwiftSocial_Props = "swiftsocial-test.props"

AllMachines = (Surrogates + Scouts + ShepardAddr).unique()

println getBinding().getVariables()

dumpTo(AllMachines, "/tmp/nodes.txt")


//    pnuke(AllMachines, "java", 60)

//System.exit(0)

println "==== BUILDING JAR..."
sh("ant -buildfile smd-jar-build.xml").waitFor()
deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, "stuff/all_logging.properties", "all_logging.properties")
deployTo(AllMachines, SwiftSocial_Props)


def shep = SwiftSocial.runShepard( ShepardAddr, Duration, "Released" )

//    SwiftSocial.runEachAsDatacentre(Surrogates, "256m", "512m")
Sleep(10)

//    println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
//println "==== INITIALIZING DATABASE ===="
//def INIT_DB_DC = Surrogates.get(0)
//def INIT_DB_CLIENT = Surrogates.get(0)
//
//SwiftSocial.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftSocial_Props)


println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
SwiftSocial.runStandaloneScoutFailOver( Scouts, Surrogates, SwiftSocial_Props, ShepardAddr, Threads )

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Remaining: ", Duration + 30)

pnuke(AllMachines, "java", 60)

def dstDir="results/swiftsocial/SOSP/FailOver/" + new Date().format('MMMdd-') + System.currentTimeMillis()
def dstFile = String.format("1pc-results-swiftsocial-DC-%s-SC-%s-TH-%s.log", Surrogates.size(), Scouts.size(), Threads)

pslurp( Scouts, "scout-stdout.txt", dstDir, dstFile, 300)

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()

System.exit(0)

