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
if (workloadName.startsWith("workload-social")) {
    exp = new SwiftSocial2()
    OPS_PER_CLIENT = workloadName.endsWith("views-counter") ? 0.5 : 1
    exp.baseWorkload = SwiftSocial2.WORKLOADS[workloadName]
} else {
    exp = new SwiftYCSB()
    OPS_PER_CLIENT = workloadName.startsWith("workloadb") ? 2.5 : 0.25
    exp.baseWorkload = SwiftYCSB.WORKLOADS[workloadName]
}
def modeName = args[2]
exp.mode = SwiftBase.MODES[modeName]
exp.dbSize = Integer.parseInt(args[3])

OBJECTS_PER_CLIENT = 20
exp.clients = exp.dbSize / OBJECTS_PER_CLIENT
exp.incomingOpPerSecLimit = (int) (OPS_PER_CLIENT * ((double) exp.clients))

def outputDir = args[4]
exp.runExperiment(String.format("%s/%s-mode-%s-dbsize-%d", outputDir, workloadName, modeName, exp.dbSize))

System.exit(0)

