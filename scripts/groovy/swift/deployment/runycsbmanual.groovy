#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.SwiftYCSB.*
import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

if (args.length != 1) {
    System.err.println "usage: runycsb.groovy <topology configuration file>"
    System.exit(1)
}

// TOPOLOGY CONFIGURATION
topologyDef = new File(args[0])
println "==== Loading topology definition from file " + topologyDef + "===="
evaluate(topologyDef)

SwiftYCSB ycsb = new SwiftYCSB()
// ycsb.duration = 60
// ycsb.dbSize = 1000
ycsb.runExperiment("results/ycsb/" + new Date().format('MMMdd-') + System.currentTimeMillis() + "-" + ycsb.version + "-" + "test")

System.exit(0)

