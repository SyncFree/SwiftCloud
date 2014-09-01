#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-77-116-181.eu-west-1.compute.amazonaws.com',
'ec2-54-77-31-235.eu-west-1.compute.amazonaws.com',
'ec2-54-77-148-222.eu-west-1.compute.amazonaws.com',
]

NVirginiaEC2 = [
    // first node is a DC, followed by two groups of 10 scouts
    'ec2-54-165-55-241.compute-1.amazonaws.com',
    'ec2-54-164-210-61.compute-1.amazonaws.com',
    'ec2-54-165-55-250.compute-1.amazonaws.com',
    'ec2-54-165-55-225.compute-1.amazonaws.com',
    'ec2-54-165-55-236.compute-1.amazonaws.com',
    'ec2-54-165-55-240.compute-1.amazonaws.com',
    'ec2-54-165-55-249.compute-1.amazonaws.com',
    'ec2-54-165-55-246.compute-1.amazonaws.com',
    'ec2-54-165-55-234.compute-1.amazonaws.com',
    'ec2-54-165-55-238.compute-1.amazonaws.com',
    'ec2-54-165-55-228.compute-1.amazonaws.com',
    'ec2-54-165-55-245.compute-1.amazonaws.com',
    'ec2-54-165-55-231.compute-1.amazonaws.com',
    'ec2-54-165-55-243.compute-1.amazonaws.com',
    'ec2-54-165-55-247.compute-1.amazonaws.com',
    'ec2-54-165-55-237.compute-1.amazonaws.com',
    'ec2-54-165-55-227.compute-1.amazonaws.com',
    'ec2-54-165-55-248.compute-1.amazonaws.com',
    'ec2-54-165-55-242.compute-1.amazonaws.com',
    'ec2-54-165-55-239.compute-1.amazonaws.com',
    'ec2-54-165-55-229.compute-1.amazonaws.com',

'ec2-54-164-185-206.compute-1.amazonaws.com',
'ec2-54-164-192-63.compute-1.amazonaws.com',
]

OregonEC2 = [
    // first node is a DC, followed by 10 scouts
    'ec2-54-68-39-125.us-west-2.compute.amazonaws.com',
    'ec2-54-68-39-122.us-west-2.compute.amazonaws.com',
    'ec2-54-68-29-132.us-west-2.compute.amazonaws.com',
    'ec2-54-68-39-124.us-west-2.compute.amazonaws.com',
    'ec2-54-68-39-139.us-west-2.compute.amazonaws.com',
    'ec2-54-68-39-133.us-west-2.compute.amazonaws.com',
    'ec2-54-68-39-138.us-west-2.compute.amazonaws.com',
    'ec2-54-68-38-75.us-west-2.compute.amazonaws.com',
    'ec2-54-68-37-59.us-west-2.compute.amazonaws.com',
    'ec2-54-68-37-35.us-west-2.compute.amazonaws.com',
    'ec2-54-68-39-126.us-west-2.compute.amazonaws.com',

'ec2-54-68-53-120.us-west-2.compute.amazonaws.com',
'ec2-54-68-44-169.us-west-2.compute.amazonaws.com',
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

