#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.SwiftYCSB.*
import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

if (args.length != 5) {
    System.err.println "usage: runycsbscalabilitydbsize.groovy <topology configuration file> <workload> <mode> <dbsize> <outputdir>"
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
ycsb.mode = SwiftBase.MODES[modeName]
ycsb.dbSize = Integer.parseInt(args[3])

OBJECTS_PER_CLIENT = 50
// TODO: update for multi-DC setup?
// TODO: update for workloada!
OPS_PER_CLIENT = workloadName.startsWith("workloadb") ? 2.5 : 2.5
int clients = ycsb.dbSize / OBJECTS_PER_CLIENT
ycsb.incomingOpPerSecLimit = (int) (OPS_PER_CLIENT * ((double) clients))
ycsb.threads = (int) (clients / ycsb.scouts.size())

def outputDir = args[4]
ycsb.runExperiment(String.format("%s/%s-mode-%s-dbsize-%d", outputDir, workloadName, modeName, ycsb.dbSize))

System.exit(0)

