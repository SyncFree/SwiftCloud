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
    // first node is a DC, followed by two groups of 10 scouts
'ec2-54-165-121-170.compute-1.amazonaws.com',

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
]

OregonEC2 = [
    // first node is a DC, followed by 10 scouts
'ec2-54-68-96-198.us-west-2.compute.amazonaws.com',

    'ec2-54-68-97-6.us-west-2.compute.amazonaws.com',
    'ec2-54-68-96-236.us-west-2.compute.amazonaws.com',
    'ec2-54-68-97-30.us-west-2.compute.amazonaws.com',
    'ec2-54-68-97-41.us-west-2.compute.amazonaws.com',
    'ec2-54-68-96-255.us-west-2.compute.amazonaws.com',
    'ec2-54-68-68-100.us-west-2.compute.amazonaws.com',
    'ec2-54-68-96-11.us-west-2.compute.amazonaws.com',
    'ec2-54-68-96-43.us-west-2.compute.amazonaws.com',
    'ec2-54-218-68-40.us-west-2.compute.amazonaws.com',
    'ec2-54-218-61-19.us-west-2.compute.amazonaws.com',
]

// TOPOLOGY
Topology.clear()

// DC_NV as a first DC - used to initialize the DB 
DC_NV = DC([NVirginiaEC2[0]], [NVirginiaEC2[0]])
DC_EU = DC([EuropeEC2[0]], [EuropeEC2[0]])
DC_OR = DC([OregonEC2[0]], [OregonEC2[0]])

ScoutsToEU = SGroup(NVirginiaEC2[1..10], DC_EU)
ScoutsToNV = SGroup(OregonEC2[1..10], DC_NV)
ScoutsToOR = SGroup(NVirginiaEC2[11..20], DC_OR)
