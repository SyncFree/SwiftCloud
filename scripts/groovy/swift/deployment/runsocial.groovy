#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.PlanetLab_3X.*
import static swift.deployment.Tools.*


def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})

SurrogateEU = 'ec2-54-72-99-205.eu-west-1.compute.amazonaws.com'
SurrogateNorthVirginia = 'ec2-54-86-227-231.compute-1.amazonaws.com'
SurrogateOregon = 'ec2-54-200-37-248.us-west-2.compute.amazonaws.com'

Surrogates = [
    SurrogateEU,
    SurrogateNorthVirginia,
    SurrogateOregon,
]

ScoutsEU = [
    'ec2-54-76-22-70.eu-west-1.compute.amazonaws.com',
    'ec2-54-76-18-79.eu-west-1.compute.amazonaws.com',
    'ec2-54-72-210-145.eu-west-1.compute.amazonaws.com'
]
ScoutsNorthVirginia = [
    'ec2-54-84-112-85.compute-1.amazonaws.com',
    'ec2-54-86-232-242.compute-1.amazonaws.com',
    'ec2-54-86-236-144.compute-1.amazonaws.com'
]
ScoutsOregon = [
    'ec2-54-200-37-250.us-west-2.compute.amazonaws.com',
    'ec2-54-200-37-249.us-west-2.compute.amazonaws.com',
    'ec2-54-200-38-2.us-west-2.compute.amazonaws.com'
]

//    Scouts = (PlanetLab_EU).unique()
// (PlanetLab_NC + PlanetLab_NV + PlanetLab_EU).unique()
Scouts = ScoutsEU  + ScoutsNorthVirginia + ScoutsOregon

ScoutsToServersMap = [:]
ScoutsEU.each { scout ->
    ScoutsToServersMap[scout] = [SurrogateNorthVirginia]
}
ScoutsNorthVirginia.each { scout ->
    ScoutsToServersMap[scout] = [SurrogateOregon]
}
ScoutsOregon.each { scout ->
    ScoutsToServersMap[scout] = [SurrogateEU]
}

ShepardAddr = Surrogates.get(0);

Threads = 4
Duration = 240
InterCmdDelay = 40
SwiftSocial_Props = "swiftsocial-test.props"


AllMachines = (Surrogates + Scouts + ShepardAddr).unique()

Version = getGitCommitId()

println getBinding().getVariables()

dumpTo(AllMachines, "/tmp/nodes.txt")


pnuke(AllMachines, "java", 60)

//System.exit(0)

println "==== BUILDING JAR for version " + Version + "..."
sh("ant -buildfile smd-jar-build.xml").waitFor()
deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, SwiftSocial_Props)
//        deployTo(AllMachines, "stuff/all_logging.properties", "all_logging.properties")


def shep = SwiftSocial.runShepard( ShepardAddr, Duration, "Released" )

SwiftSocial.runEachAsDatacentre(Surrogates, "256m", "3096m")

println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
Sleep(InterCmdDelay)
println "==== INITIALIZING DATABASE ===="
def INIT_DB_DC = Surrogates.get(0)
def INIT_DB_CLIENT = Surrogates.get(0)

SwiftSocial.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftSocial_Props)


println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
Sleep(InterCmdDelay)
SwiftSocial.runStandaloneScouts( Scouts, ScoutsToServersMap, SwiftSocial_Props, ShepardAddr, Threads )

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Max. remaining time: ", Duration + InterCmdDelay)

pnuke(AllMachines, "java", 60)

def dstDir="results/swiftsocial/" + new Date().format('MMMdd-') + System.currentTimeMillis() + "-" + Version
def dstFile = String.format("DC-%s-SC-%s-TH-%s.log", Surrogates.size(), Scouts.size(), Threads)

pslurp( Scouts, "scout-stdout.txt", dstDir, dstFile, 300)

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()

System.exit(0)

