#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-76-211-102.eu-west-1.compute.amazonaws.com',

'ec2-54-77-194-154.eu-west-1.compute.amazonaws.com',
'ec2-54-77-164-206.eu-west-1.compute.amazonaws.com',
]

NVirginiaEC2 = [
    // first node is a DC, followed by two groups of 10 scouts
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
    'ec2-54-165-65-151.compute-1.amazonaws.com',
    'ec2-54-165-65-80.compute-1.amazonaws.com',
    'ec2-54-165-70-63.compute-1.amazonaws.com',
    'ec2-54-84-139-143.compute-1.amazonaws.com',
    'ec2-54-165-20-214.compute-1.amazonaws.com',
    'ec2-54-165-31-202.compute-1.amazonaws.com',
    'ec2-54-165-71-188.compute-1.amazonaws.com',
    'ec2-54-165-57-169.compute-1.amazonaws.com',
    'ec2-54-165-64-120.compute-1.amazonaws.com',
    'ec2-54-164-5-227.compute-1.amazonaws.com',
    'ec2-54-165-69-166.compute-1.amazonaws.com',

'ec2-54-165-85-26.compute-1.amazonaws.com',
'ec2-54-165-83-240.compute-1.amazonaws.com',
]

OregonEC2 = [
    // first node is a DC, followed by 10 scouts
    'ec2-54-68-0-214.us-west-2.compute.amazonaws.com',
    'ec2-54-68-57-136.us-west-2.compute.amazonaws.com',
    'ec2-54-68-87-45.us-west-2.compute.amazonaws.com',
    'ec2-54-68-99-84.us-west-2.compute.amazonaws.com',
    'ec2-54-68-104-66.us-west-2.compute.amazonaws.com',
    'ec2-54-68-98-237.us-west-2.compute.amazonaws.com',
    'ec2-54-68-100-193.us-west-2.compute.amazonaws.com',
    'ec2-54-68-92-16.us-west-2.compute.amazonaws.com',
    'ec2-54-68-79-187.us-west-2.compute.amazonaws.com',
    'ec2-54-68-89-81.us-west-2.compute.amazonaws.com',
    'ec2-54-68-66-55.us-west-2.compute.amazonaws.com',

'ec2-54-68-105-173.us-west-2.compute.amazonaws.com',
'ec2-54-68-105-146.us-west-2.compute.amazonaws.com',
]

// TOPOLOGY
Topology.clear()

// DC_NV as a first DC - used to initialize the DB 
DC_NV = DC([NVirginiaEC2[0]], [NVirginiaEC2[0]])
DC_EU = DC([EuropeEC2[0]], [EuropeEC2[0]])
DC_OR = DC([OregonEC2[0]], [OregonEC2[0]])
DC_NV2 = DC([NVirginiaEC2[21]], [NVirginiaEC2[21]])
DC_EU2 = DC([EuropeEC2[1]], [EuropeEC2[1]])
DC_OR2 = DC([OregonEC2[11]], [OregonEC2[11]])

ScoutsToEU = SGroup(NVirginiaEC2[1..5], DC_EU)
ScoutsToEU2 = SGroup(NVirginiaEC2[6..10], DC_EU2)
ScoutsToNV = SGroup(OregonEC2[1..5], DC_NV)
ScoutsToNV2 = SGroup(OregonEC2[6..10], DC_NV2)
ScoutsToOR = SGroup(NVirginiaEC2[11..15], DC_OR)
ScoutsToOR2 = SGroup(NVirginiaEC2[16..20], DC_OR2)

