#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-77-131-169.eu-west-1.compute.amazonaws.com'
]

NVirginiaEC2 = [
    // first node is a DC, followed by two groups of 6 and 7 scouts
    'ec2-54-164-126-219.compute-1.amazonaws.com',
    'ec2-54-164-125-149.compute-1.amazonaws.com',
    'ec2-54-164-121-180.compute-1.amazonaws.com',
    'ec2-54-164-122-55.compute-1.amazonaws.com',
    'ec2-54-164-125-119.compute-1.amazonaws.com',
    'ec2-54-164-69-240.compute-1.amazonaws.com',
    'ec2-54-164-122-87.compute-1.amazonaws.com',
    'ec2-54-164-124-122.compute-1.amazonaws.com',
    'ec2-54-164-122-158.compute-1.amazonaws.com',
    'ec2-54-85-20-253.compute-1.amazonaws.com',
    'ec2-54-164-120-209.compute-1.amazonaws.com',
    'ec2-54-164-120-104.compute-1.amazonaws.com',
    'ec2-54-164-88-33.compute-1.amazonaws.com',
    'ec2-54-164-121-226.compute-1.amazonaws.com'
]

OregonEC2 = [
    // first node is a DC, followed by 7 scouts
    'ec2-54-213-49-129.us-west-2.compute.amazonaws.com',
    'ec2-54-213-50-168.us-west-2.compute.amazonaws.com',
    'ec2-54-201-84-45.us-west-2.compute.amazonaws.com',
    'ec2-54-201-96-253.us-west-2.compute.amazonaws.com',
    'ec2-54-213-50-237.us-west-2.compute.amazonaws.com',
    'ec2-54-213-42-22.us-west-2.compute.amazonaws.com',
    'ec2-54-201-156-10.us-west-2.compute.amazonaws.com',
    'ec2-54-201-173-208.us-west-2.compute.amazonaws.com'
]

// TOPOLOGY
Topology.clear()

// DC_NV as a first DC - used to initialize the DB 
DC_NV = DC([NVirginiaEC2[0]], [NVirginiaEC2[0]])
DC_EU = DC([EuropeEC2[0]], [EuropeEC2[0]])
DC_OR = DC([OregonEC2[0]], [OregonEC2[0]])

ScoutsToEU = SGroup(NVirginiaEC2[1..6], DC_EU)
ScoutsToNV = SGroup(OregonEC2[1..7], DC_NV)
ScoutsToOR = SGroup(NVirginiaEC2[7..13], DC_OR)
