#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.SwiftYCSB.*
import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

if (args.length < 5) {
    System.err.println "usage: runscalabilityclients.groovy <topology configuration file> <workload> <mode> <clients_number> <outputdir> [dbsize]"
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
if (args.length > 5) {
    exp.dbSize = args[5].toInteger()
} 
if (workloadName.startsWith("workload-social")) {
    exp = new SwiftSocial2()
    exp.baseWorkload = SwiftSocial2.WORKLOADS[workloadName]
    exp.mode = SwiftBase.MODES[modeName]
    if (workloadName.endsWith("views-counter")) {
        exp.incomingOpPerSecLimit = 1500
        if (exp.dbSize == 10000) {
            exp.incomingOpPerSecLimit = 800
        }
    } else {
        exp.incomingOpPerSecLimit = 3000
        if (exp.dbSize == 10000) {
            exp.incomingOpPerSecLimit = 1000
        }
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
        if (exp.dbSize == 10000) {
            exp.incomingOpPerSecLimit = 1000
        }
    }
}

exp.clients = Integer.parseInt(args[3])
def outputDir = args[4]
exp.runExperiment(outputDir)

System.exit(0)
