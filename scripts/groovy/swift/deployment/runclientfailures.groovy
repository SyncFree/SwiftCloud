#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.SwiftYCSB.*
import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

if (args.length < 5) {
    System.err.println "usage: runclientfailures.groovy <topology file> <workload> <mode> <failures number> <outputdir>"
    System.exit(1)
}

// TOPOLOGY CONFIGURATION
topologyDef = new File(args[0])
println "==== Loading topology definition from file " + topologyDef + "===="
evaluate(topologyDef)

// VARs
def workloadName = args[1]
def exp
if (workloadName.startsWith("workload-social")) {
    exp = new SwiftSocial2()
    exp.baseWorkload = SwiftSocial2.WORKLOADS[workloadName]
    // results-backwards compatibility hack(!)
    exp.baseWorkload += LEGACY_HIGHLOCALITY
} else {
    exp = new SwiftYCSB()
    exp.baseWorkload = SwiftYCSB.WORKLOADS[workloadName]
}
def modeName = args[2]
exp.mode = SwiftBase.MODES[modeName]
def failures  = args[3].toInteger()
exp.dbSize = 10000
exp.clients = 500
exp.incomingOpPerSecLimit = 1000
def outputDir = args[4]
def outputDirs = []
for (int accFailures = 0; accFailures <= failures; accFailures += exp.clients) {
    outputDirs += String.format("%s/%s-mode-%s-failures-%d", outputDir, workloadName, modeName, accFailures)
}

exp.runExperiment(*outputDirs)

System.exit(0)

