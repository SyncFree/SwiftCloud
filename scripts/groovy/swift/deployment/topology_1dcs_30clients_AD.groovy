#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-72-3-142.eu-west-1.compute.amazonaws.com',
]

NVirginiaEC2 = [
    // clients
'ec2-54-165-23-103.compute-1.amazonaws.com',
'ec2-54-165-26-186.compute-1.amazonaws.com',
'ec2-54-165-28-56.compute-1.amazonaws.com',
'ec2-54-165-22-141.compute-1.amazonaws.com',
'ec2-54-165-29-38.compute-1.amazonaws.com',
'ec2-54-165-22-147.compute-1.amazonaws.com',
'ec2-54-165-23-2.compute-1.amazonaws.com',
'ec2-54-165-29-154.compute-1.amazonaws.com',
'ec2-54-165-22-4.compute-1.amazonaws.com',
'ec2-54-165-28-209.compute-1.amazonaws.com',

'ec2-54-165-22-168.compute-1.amazonaws.com',
'ec2-54-165-26-255.compute-1.amazonaws.com',
'ec2-54-165-25-232.compute-1.amazonaws.com',
'ec2-54-165-22-125.compute-1.amazonaws.com',
'ec2-54-165-29-12.compute-1.amazonaws.com',
'ec2-54-165-29-33.compute-1.amazonaws.com',
'ec2-54-165-27-40.compute-1.amazonaws.com',
'ec2-54-165-30-105.compute-1.amazonaws.com',
'ec2-54-165-26-63.compute-1.amazonaws.com',
'ec2-54-165-31-209.compute-1.amazonaws.com',

'ec2-54-165-81-214.compute-1.amazonaws.com',
'ec2-54-165-73-21.compute-1.amazonaws.com',
'ec2-54-165-86-111.compute-1.amazonaws.com',
'ec2-54-165-86-101.compute-1.amazonaws.com',
'ec2-54-164-255-168.compute-1.amazonaws.com',
'ec2-54-165-64-238.compute-1.amazonaws.com',
'ec2-54-165-73-136.compute-1.amazonaws.com',
'ec2-54-165-83-114.compute-1.amazonaws.com',
'ec2-54-165-64-253.compute-1.amazonaws.com',
'ec2-54-165-70-112.compute-1.amazonaws.com',
]

// TOPOLOGY
Topology.clear()

DC_EU = DC([EuropeEC2[0]], [EuropeEC2[0]])

ScoutsToEU = SGroup(NVirginiaEC2, DC_EU)
