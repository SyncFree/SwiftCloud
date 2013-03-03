#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib

import static Tools.*

def __ = onControlC({
    pnuke(AllMachines, "java").waitFor()
    System.exit(0);
})


Surrogates = [
"peeramide.irisa.fr"
]

EndClients = [
	"planetlab1.di.fct.unl.pt",
	"planetlab2.di.fct.unl.pt",
    "planetlab1.fct.ualg.pt",
    "planetlab2.fct.ualg.pt"
]

Shepard = "utet.ii.uam.es"

def Threads = 3
def Duration = 120
def SwiftSocial_Props = "swiftsocial-test.props"

def Scouts = Surrogates

AllMachines = (Surrogates + EndClients + Shepard).unique()

pnuke(AllMachines, "java").waitFor()



println "==== BUILDING JAR..."

sh("ant -buildfile smd-jar-build.xml").waitFor()


deployTo(AllMachines, "swiftcloud.jar").waitFor()
deployTo(AllMachines, "stuff/all_logging.properties", "all_logging.properties").waitFor()
deployTo(AllMachines, SwiftSocial_Props).waitFor()


def shep = SwiftSocial.runShepard( Shepard, Duration, "Released" )

SwiftSocial.runEachAsDatacentre(Surrogates, "128m", "512m")

println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
Sleep(10)


println "==== INITIALIZING DATABASE ===="
def INIT_DB_DC = Surrogates.get(0)
def INIT_DB_CLIENT = Surrogates.get(0)

SwiftSocial.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftSocial_Props)


println "==== WAITING A BIT BEFORE STARTING SERVER SCOUTS ===="
Sleep(10)

SwiftSocial.runCS_ServerScouts( Scouts, ["localhost"], SwiftSocial_Props, "-cache 0")

println "==== WAITING A BIT BEFORE STARTING ENDCLIENTS ===="
Sleep(10)

SwiftSocial.runCS_EndClients( EndClients, Scouts, SwiftSocial_Props, Shepard, Threads )


println "==== WAITING FOR SHEPARD TO INITIATE COUNTDOWN ===="
shep.take()

Countdown( "Remaining: ", Duration + 30)

pnuke(AllMachines, "java").waitFor()

def dstDir="results/swiftsocial/" + new Date().format('MMMdd-') + System.currentTimeMillis()
def dstFile = String.format("1pc-results-swiftsocial-DC-%s-SC-%s-TH-%s.log", Surrogates.size(), Scouts.size(), Threads)
def prefix = dstDir + "/" + dstFile

new File( dstDir).mkdirs()
pslurp( EndClients, HOMEDIR + "client-stdout.txt", dstFile, prefix).waitFor()

sh("ls -lR " + dstDir).waitFor();


System.exit(0)

