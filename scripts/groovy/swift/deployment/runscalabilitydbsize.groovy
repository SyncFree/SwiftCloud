#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.SwiftYCSB.*
import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

if (args.length != 5) {
    System.err.println "usage: runscalabilitydbsize.groovy <topology configuration file> <workload> <mode> <dbsize> <outputdir>"
    System.exit(1)
}

// TOPOLOGY CONFIGURATION
topologyDef = new File(args[0])
println "==== Loading topology definition from file " + topologyDef + "===="
evaluate(topologyDef)


// VARs
def workloadName = args[1]
def exp
double OPS_PER_CLIENT
def modeName = args[2]
if (workloadName.startsWith("workload-social")) {
    exp = new SwiftSocial2()
    exp.mode = SwiftBase.MODES[modeName]
    exp.baseWorkload = SwiftSocial2.WORKLOADS[workloadName]
    if (workloadName.endsWith("views-counter")) {
        OPS_PER_CLIENT = 0.5
    } else {
        OPS_PER_CLIENT = 1
    }
} else {
    exp = new SwiftYCSB()
    exp.mode = SwiftBase.MODES[modeName]
    exp.baseWorkload = SwiftYCSB.WORKLOADS[workloadName]
    if (workloadName.startsWith("workloada")) {
        // a bigger cache needs too much time to get warm
        exp.mode['swift.cacheSize'] = '64'
        exp.localRecordCount = 48
        OPS_PER_CLIENT = 0.25
    } else {
        OPS_PER_CLIENT = 2.5
    }
}
exp.dbSize = Integer.parseInt(args[3])

OBJECTS_PER_CLIENT = 20
exp.clients = exp.dbSize / OBJECTS_PER_CLIENT
exp.incomingOpPerSecLimit = (int) (OPS_PER_CLIENT * ((double) exp.clients))

def outputDir = args[4]
exp.runExperiment(String.format("%s/%s-mode-%s-dbsize-%d", outputDir, workloadName, modeName, exp.dbSize))

System.exit(0)

