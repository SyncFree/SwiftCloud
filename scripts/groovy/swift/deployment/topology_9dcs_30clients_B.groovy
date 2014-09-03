#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-77-56-23.eu-west-1.compute.amazonaws.com',
'ec2-54-77-111-18.eu-west-1.compute.amazonaws.com',
'ec2-54-77-132-17.eu-west-1.compute.amazonaws.com',
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

'ec2-54-165-84-39.compute-1.amazonaws.com',
'ec2-54-165-85-112.compute-1.amazonaws.com',
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

'ec2-54-68-105-168.us-west-2.compute.amazonaws.com',
'ec2-54-68-104-197.us-west-2.compute.amazonaws.com',
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
DC_NV3 = DC([NVirginiaEC2[22]], [NVirginiaEC2[22]])
DC_EU3 = DC([EuropeEC2[2]], [EuropeEC2[2]])
DC_OR3 = DC([OregonEC2[12]], [OregonEC2[12]])

ScoutsToEU = SGroup(NVirginiaEC2[1..4], DC_EU)
ScoutsToEU2 = SGroup(NVirginiaEC2[5..7], DC_EU2)
ScoutsToEU3 = SGroup(NVirginiaEC2[8..10], DC_EU3)
ScoutsToNV = SGroup(OregonEC2[1..4], DC_NV)
ScoutsToNV2 = SGroup(OregonEC2[5..7], DC_NV2)
ScoutsToNV3 = SGroup(OregonEC2[8..10], DC_NV3)
ScoutsToOR = SGroup(NVirginiaEC2[11..14], DC_OR)
ScoutsToOR2 = SGroup(NVirginiaEC2[15..17], DC_OR2)
ScoutsToOR3 = SGroup(NVirginiaEC2[18..20], DC_OR3)

