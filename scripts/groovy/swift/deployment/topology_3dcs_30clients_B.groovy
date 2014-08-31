#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-77-56-23.eu-west-1.compute.amazonaws.com',
]

NVirginiaEC2 = [
    // first node is a DC, followed by two groups of 10 scouts, and 2 DCs
'ec2-54-164-130-184.compute-1.amazonaws.com',
'ec2-54-164-81-205.compute-1.amazonaws.com',
'ec2-54-164-177-141.compute-1.amazonaws.com',
'ec2-54-164-197-149.compute-1.amazonaws.com',
'ec2-54-164-145-107.compute-1.amazonaws.com',
'ec2-54-164-152-72.compute-1.amazonaws.com',
'ec2-54-164-191-226.compute-1.amazonaws.com',
'ec2-54-85-203-9.compute-1.amazonaws.com',
'ec2-54-164-65-77.compute-1.amazonaws.com',
'ec2-54-164-154-114.compute-1.amazonaws.com',
'ec2-54-164-200-3.compute-1.amazonaws.com',
'ec2-54-164-115-108.compute-1.amazonaws.com',
'ec2-54-164-145-151.compute-1.amazonaws.com',
'ec2-54-164-73-233.compute-1.amazonaws.com',
'ec2-54-164-160-34.compute-1.amazonaws.com',
'ec2-54-164-196-159.compute-1.amazonaws.com',
'ec2-54-164-93-80.compute-1.amazonaws.com',
'ec2-54-164-136-109.compute-1.amazonaws.com',
'ec2-54-164-200-186.compute-1.amazonaws.com',
'ec2-54-164-200-6.compute-1.amazonaws.com',
'ec2-54-164-102-39.compute-1.amazonaws.com',
]

OregonEC2 = [
    // first node is a DC, followed by 10 scouts, and 2 DCs
    'ec2-54-68-4-179.us-west-2.compute.amazonaws.com',
    'ec2-54-68-4-180.us-west-2.compute.amazonaws.com',
'ec2-54-68-4-181.us-west-2.compute.amazonaws.com',
'ec2-54-68-4-182.us-west-2.compute.amazonaws.com',
'ec2-54-68-4-183.us-west-2.compute.amazonaws.com',
'ec2-54-68-4-184.us-west-2.compute.amazonaws.com',
'ec2-54-68-4-185.us-west-2.compute.amazonaws.com',
'ec2-54-68-4-187.us-west-2.compute.amazonaws.com',
'ec2-54-68-4-188.us-west-2.compute.amazonaws.com',
'ec2-54-68-4-189.us-west-2.compute.amazonaws.com',
'ec2-54-68-4-190.us-west-2.compute.amazonaws.com',
]

// TOPOLOGY
Topology.clear()

DC_NV = DC([NVirginiaEC2[0]], [NVirginiaEC2[0]])
DC_EU = DC([EuropeEC2[0]], [EuropeEC2[0]])
DC_OR = DC([OregonEC2[0]], [OregonEC2[0]])

ScoutsToEU = SGroup(NVirginiaEC2[1..10], DC_EU)
ScoutsToNV = SGroup(OregonEC2[1..10], DC_NV)
ScoutsToOR = SGroup(NVirginiaEC2[11..20], DC_OR)

