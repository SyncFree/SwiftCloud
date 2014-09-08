#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.SwiftYCSB.*
import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

if (args.length < 5) {
    System.err.println "usage: runstaleness.groovy <topology file> <workload> <mode> <dbsize> <outputdir>"
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
} else {
    exp = new SwiftYCSB()
    exp.baseWorkload = SwiftYCSB.WORKLOADS[workloadName]
}
def modeName = args[2]
exp.mode = SwiftBase.MODES[modeName]
exp.dbSize = args[3].toInteger()
exp.clients = 500
exp.incomingOpPerSecLimit = 1000
exp.reports += ['STALENESS_YCSB_READ', 'STALENESS_READ', 'STALENESS_CALIB']
def outputDir = args[4]
exp.runExperiment(outputDir)

System.exit(0)

