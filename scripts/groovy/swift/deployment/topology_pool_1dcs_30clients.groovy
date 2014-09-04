#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

evaluate(Topology.acquireTopologyFile("topology_1dcs_30clients"))
