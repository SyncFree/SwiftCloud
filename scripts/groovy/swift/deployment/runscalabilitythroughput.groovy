#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.SwiftYCSB.*
import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

if (args.length != 5) {
    System.err.println "usage: runycscalabilitythroughput.groovy <topology file> <workload> <mode> <opslimit> <outputdir>"
    System.exit(1)
}

// TOPOLOGY CONFIGURATION
topologyDef = new File(args[0])
println "==== Loading topology definition from file " + topologyDef + "===="
evaluate(topologyDef)

// VARs
def workloadName = args[1]
def exp
if (workloadName.st("workload-social")) {
    exp = new SwiftSocial2()
    exp.baseWorkload = SwiftSocial2.WORKLOADS[workloadName]
} else {
    exp = new SwiftYCSB()
    exp.baseWorkload = SwiftYCSB.WORKLOADS[workloadName]
}
def modeName = args[2]
exp.mode = SwiftBase.MODES[modeName]
exp.incomingOpPerSecLimit  = Integer.parseInt(args[3])
exp.threads = 40
def outputDir = args[4]
exp.runExperiment(String.format("%s/%s-mode-%s-opslimit-%d", outputDir, workloadName, modeName, exp.incomingOpPerSecLimit))

System.exit(0)

