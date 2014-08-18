#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.SwiftYCSB.*
import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

if (args.length != 5) {
    System.err.println "usage: runycsbscalabilitythroughput.groovy <topology file> <workload> <mode> <opslimit> <outputdir>"
    System.exit(1)
}

// TOPOLOGY CONFIGURATION
topologyDef = new File(args[0])
println "==== Loading topology definition from file " + topologyDef + "===="
evaluate(topologyDef)

SwiftYCSB ycsb = new SwiftYCSB()
// VARs
def workloadName = args[1]
ycsb.baseWorkload = SwiftYCSB.WORKLOADS[workloadName]
def modeName = args[2]
ycsb.mode = SwiftYCSB.MODES[modeName]
ycsb.incomingOpPerSecLimit  = Integer.parseInt(args[3])
ycsb.threads = 40
def outputDir = args[4]
ycsb.runExperiment(String.format("%s/%s-mode-%s-opslimit-%d", outputDir, workloadName, modeName, ycsb.incomingOpPerSecLimit))

System.exit(0)

