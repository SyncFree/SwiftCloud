#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;
import java.lang.management.ManagementFactory;

WAIT_MAX_MS = 2000

random = new Random();
File acquiredConfig = null
File acquiredConfigRenamed = null
pid = ManagementFactory.getRuntimeMXBean().getName()
while (acquiredConfig == null) {
    // Everyone needs to wait in the first place to achieve fairness
    sleep(random.nextInt(WAIT_MAX_MS))
    for (File child : new File("scripts/groovy/swift/deployment/").listFiles()) {
        if (child.name.startsWith("topology_multidc") && child.name.endsWith(".groovy")) {
            File candidateConfigRenamed = new File(child.absolutePath + ".locked." + pid)
            if (child.renameTo(candidateConfigRenamed)) {
                acquiredConfig = child
                acquiredConfigRenamed = candidateConfigRenamed
                break
            }
        }
    }
    if (acquiredConfig == null) {
        println("No topology configuration available - retrying in max. " + WAIT_MAX_MS + "ms")
    }
}

println "Topology configuration " + acquiredConfig + " acquired by process " + pid
addShutdownHook {
    if (acquiredConfigRenamed.renameTo(acquiredConfig)) {
        println "Topology configuration " + acquiredConfig + " released"
    } else {
        println "WARNING: could not release topology configuration " + acquiredConfig " acuired by process" + pid
    }
}
evaluate(acquiredConfigRenamed)
