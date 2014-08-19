#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

if (args.length != 1) {
    System.err.println "usage: runsocialmanual.groovy <topology configuration file>"
    System.exit(1)
}

// TOPOLOGY CONFIGURATION
topologyDef = new File(args[0])
println "==== Loading topology definition from file " + topologyDef + "===="
evaluate(topologyDef)

SwiftSocial2 social = new SwiftSocial2()
social.dbSize = social.scouts.size() * social.threads * 200
social.runExperiment("results/swiftsocial/" + new Date().format('MMMdd-') + System.currentTimeMillis() + "-" + social.version + "-" + "test")

System.exit(0)

