#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.SwiftYCSB.*
import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

if (args.length != 5) {
    System.err.println "usage: runscalabilityclients.groovy <topology configuration file> <workload> <mode> <clients_number> <outputdir>"
    System.exit(1)
}

// TOPOLOGY CONFIGURATION
topologyDef = new File(args[0])
println "==== Loading topology definition from file " + topologyDef + "===="
evaluate(topologyDef)

// VARs
def workloadName = args[1]
def exp
def modeName = args[2]
if (workloadName.startsWith("workload-social")) {
    exp = new SwiftSocial2()
    exp.baseWorkload = SwiftSocial2.WORKLOADS[workloadName]
    exp.mode = SwiftBase.MODES[modeName]
    if (workloadName.endsWith("views-counter")) {
        exp.incomingOpPerSecLimit = 1500
    } else {
        exp.incomingOpPerSecLimit = 3000
    }
} else {
    exp = new SwiftYCSB()
    exp.baseWorkload = SwiftYCSB.WORKLOADS[workloadName]
    exp.mode = SwiftBase.MODES[modeName]
    if (workloadName.startsWith("workloada")) {
        exp.incomingOpPerSecLimit = 400
        exp.mode['swift.cacheSize'] = '64'
        exp.localRecordCount = 48
    } else {
        exp.incomingOpPerSecLimit = 4000
    }
}
exp.clients = Integer.parseInt(args[3])
exp.dbSize = 1000
def outputDir = args[4]
exp.runExperiment(String.format("%s/%s-mode-%s-clients-%d", outputDir, workloadName, modeName, exp.clients))

System.exit(0)
