#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib

import static Tools.*


def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})

int Duration = 90

Surrogates = [
    'peeramide.irisa.fr'
]

Scouts = [
"planetlab1.di.fct.unl.pt","planetlab2.di.fct.unl.pt"    
]

AllMachines = (Surrogates + Scouts).unique()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)

//System.exit(0)

println "==== BUILDING JAR..."
sh("ant -buildfile smd-jar-build.xml").waitFor()
deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, "evaluation/swiftdoc/swiftdoc-patches.zip", "swiftdoc-patches.zip")
//deployTo(AllMachines, "stuff/all_logging.properties", "all_logging.properties")

SwiftDoc.runEachAsDatacentre(Surrogates, "256m", "512m")
Sleep(10)

println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
SwiftDoc.runStandaloneScouts( Scouts, Surrogates.get(0), "1", "REPEATABLE_READS", "CACHED", "true" )


Countdown( "Remaining: ", Duration + 30)

pnuke(AllMachines, "java", 60)

def dstDir="results/swiftdoc/" + new Date().format('MMMdd-') + System.currentTimeMillis()
def dstFile = String.format("1pc-results-swiftdoc-DC-%s-SC-%s.log", Surrogates.size(), Scouts.size())

pslurp( Scouts, "scout-stdout.txt", dstDir, dstFile, 300)

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()

System.exit(0)

