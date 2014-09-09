#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

if (args.length < 1) {
    System.err.println "usage: shutdown.groovy <topology file> [topology2 topology3 ...]"
    System.exit(1)
}

for (String arg : args) {
	topologyDef = new File(arg)
	println "==== Loading topology definition from file " + topologyDef + "===="
	evaluate(topologyDef)
        println "==== Shuttding down " + Topology.allMachines().size() + " machines ===="
	Tools.shutdown(Topology.allMachines())
}

System.exit(0)

