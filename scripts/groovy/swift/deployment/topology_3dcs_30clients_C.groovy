#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-77-85-135.eu-west-1.compute.amazonaws.com',
]

NVirginiaEC2 = [
    // first node is a DC, followed by two groups of 10 scouts
'ec2-54-165-26-43.compute-1.amazonaws.com',

'ec2-54-165-26-176.compute-1.amazonaws.com',
'ec2-54-165-24-124.compute-1.amazonaws.com',
'ec2-54-165-26-189.compute-1.amazonaws.com',
'ec2-54-165-23-9.compute-1.amazonaws.com',
'ec2-54-165-25-37.compute-1.amazonaws.com',
'ec2-54-165-26-146.compute-1.amazonaws.com',
'ec2-54-165-24-102.compute-1.amazonaws.com',
'ec2-54-86-250-240.compute-1.amazonaws.com',
'ec2-54-165-24-137.compute-1.amazonaws.com',
'ec2-54-165-23-33.compute-1.amazonaws.com',

'ec2-54-165-25-126.compute-1.amazonaws.com',
'ec2-54-165-26-175.compute-1.amazonaws.com',
'ec2-54-165-23-249.compute-1.amazonaws.com',
'ec2-54-86-222-44.compute-1.amazonaws.com',
'ec2-54-165-26-151.compute-1.amazonaws.com',
'ec2-54-165-25-227.compute-1.amazonaws.com',
'ec2-54-165-26-170.compute-1.amazonaws.com',
'ec2-54-165-26-159.compute-1.amazonaws.com',
'ec2-54-165-25-51.compute-1.amazonaws.com',
'ec2-54-165-23-167.compute-1.amazonaws.com',
]

OregonEC2 = [
    // first node is a DC, followed by 10 scouts
'ec2-54-68-15-53.us-west-2.compute.amazonaws.com',
'ec2-54-68-20-19.us-west-2.compute.amazonaws.com',
'ec2-54-68-11-143.us-west-2.compute.amazonaws.com',
'ec2-54-68-12-69.us-west-2.compute.amazonaws.com',
'ec2-54-68-7-141.us-west-2.compute.amazonaws.com',
'ec2-54-68-15-153.us-west-2.compute.amazonaws.com',
'ec2-54-68-22-54.us-west-2.compute.amazonaws.com',
'ec2-54-68-11-253.us-west-2.compute.amazonaws.com',
'ec2-54-68-22-53.us-west-2.compute.amazonaws.com',
'ec2-54-68-12-53.us-west-2.compute.amazonaws.com',
'ec2-54-68-18-69.us-west-2.compute.amazonaws.com',
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
