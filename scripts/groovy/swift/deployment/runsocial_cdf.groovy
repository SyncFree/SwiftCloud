#!/usr/bin/env groovy -classpath .:scripts/groovy
package swift.deployment

import static swift.deployment.Tools.*

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})

Surrogates = [
    "ec2-54-247-45-87.eu-west-1.compute.amazonaws.com",
    "ec2-54-245-47-163.us-west-2.compute.amazonaws.com"
]

PlanetLab_NV = [
    "pl2.cs.unm.edu",
    //   "pl3.cs.unm.edu",
    //   "pl4.cs.unm.edu"
]

Scouts = (PlanetLab_NV).unique()

ShepardAddr = Surrogates.get(0);

Threads = 1
Duration = 120
SwiftSocial_Props = "swiftsocial-test.props"

AllMachines = (Surrogates + Scouts + ShepardAddr).unique()

println getBinding().getVariables()

dumpTo(AllMachines, "/tmp/nodes.txt")


pnuke(AllMachines, "java", 60)

//System.exit(0)

println "==== BUILDING JAR..."
sh("ant -buildfile smd-jar-build.xml").waitFor()
deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, "stuff/all_logging.properties", "all_logging.properties")
deployTo(AllMachines, SwiftSocial_Props)


def shep = SwiftSocial.runShepard( ShepardAddr, Duration, "Released" )

SwiftSocial.runEachAsDatacentre(Surrogates, "256m", "3096m")
Sleep(10)

//    println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
//println "==== INITIALIZING DATABASE ===="
//def INIT_DB_DC = Surrogates.get(0)
//def INIT_DB_CLIENT = Surrogates.get(0)
//
//SwiftSocial.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftSocial_Props)


println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
SwiftSocial.runStandaloneScouts( Scouts, Surrogates, SwiftSocial_Props, ShepardAddr, Threads )

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Remaining: ", Duration + 30)

pnuke(AllMachines, "java", 60)

def dstDir="results/swiftsocial/" + new Date().format('MMMdd-') + System.currentTimeMillis() + "/cdf"
def dstFile = String.format("1pc-results-swiftsocial-DC-%s-SC-%s-TH-%s.log", Surrogates.size(), Scouts.size(), Threads)

pslurp( Scouts, "scout-stdout.txt", dstDir, dstFile, 300)

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()

System.exit(0)

