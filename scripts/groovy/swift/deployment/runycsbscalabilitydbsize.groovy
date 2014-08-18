#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.SwiftYCSB.*
import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(1);
})

if (args.length != 5) {
    System.err.println "usage: runycsb.groovy <topology configuration file> <workload> <mode> <dbsize> <outputdir>"
    System.exit(1)
}

// TOPOLOGY CONFIGURATION
topologyDef = new File(args[0])
println "==== Loading topology definition from file " + topologyDef + "===="
evaluate(topologyDef)
Scouts = ( Topology.scouts() ).unique()
ShepardAddr = Topology.datacenters[0].surrogates[0];
AllMachines = ( Topology.allMachines() + ShepardAddr).unique()


SwiftYCSB ycsb = new SwiftYCSB()

// VARs
def workloadName = args[1]
ycsb.baseWorkload = SwiftYCSB.WORKLOADS[workloadName]
def modeName = args[2]
ycsb.mode = SwiftBase.MODES[modeName]
ycsb.dbSize = Integer.parseInt(args[3])

OBJECTS_PER_CLIENT = 50
OPS_PER_CLIENT = 2.5
int clients = ycsb.dbSize / OBJECTS_PER_CLIENT
ycsb.incomingOpPerSecLimit = (int) (OPS_PER_CLIENT * ((double) clients))
ycsb.threads = (int) (clients / ycsb.scouts.size())

def outputDir = args[4]
ycsb.runExperiment(String.format("%s/%s-mode-%s-dbsize-%d", outputDir, workloadName, modeName, ycsb.dbSize))

System.exit(0)

