#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-77-127-118.eu-west-1.compute.amazonaws.com',
]

NVirginiaEC2 = [
    // first node is a DC, followed by two groups of 10 scouts
    'ec2-54-165-12-127.compute-1.amazonaws.com',
    'ec2-54-165-35-39.compute-1.amazonaws.com',
    'ec2-54-165-22-237.compute-1.amazonaws.com',
    'ec2-54-165-19-168.compute-1.amazonaws.com',
    'ec2-54-165-22-77.compute-1.amazonaws.com',
    'ec2-54-165-7-227.compute-1.amazonaws.com',
    'ec2-54-165-28-54.compute-1.amazonaws.com',
    'ec2-54-165-32-118.compute-1.amazonaws.com',
    'ec2-54-165-23-168.compute-1.amazonaws.com',
    'ec2-54-165-31-168.compute-1.amazonaws.com',
    'ec2-54-165-32-25.compute-1.amazonaws.com',
    'ec2-54-165-19-46.compute-1.amazonaws.com',
    'ec2-54-165-20-72.compute-1.amazonaws.com',
    'ec2-54-165-39-116.compute-1.amazonaws.com',
    'ec2-54-165-23-9.compute-1.amazonaws.com',
    'ec2-54-165-20-232.compute-1.amazonaws.com',
    'ec2-54-165-16-206.compute-1.amazonaws.com',
    'ec2-54-165-22-188.compute-1.amazonaws.com',
    'ec2-54-165-21-148.compute-1.amazonaws.com',
    'ec2-54-165-20-218.compute-1.amazonaws.com',
    'ec2-54-165-6-226.compute-1.amazonaws.com',
]

OregonEC2 = [
    // first node is a DC, followed by 10 scouts
    'ec2-54-68-39-219.us-west-2.compute.amazonaws.com',
    'ec2-54-68-38-186.us-west-2.compute.amazonaws.com',
    'ec2-54-68-40-16.us-west-2.compute.amazonaws.com',
    'ec2-54-68-40-5.us-west-2.compute.amazonaws.com',
    'ec2-54-68-40-17.us-west-2.compute.amazonaws.com',
    'ec2-54-68-40-18.us-west-2.compute.amazonaws.com',
    'ec2-54-68-40-47.us-west-2.compute.amazonaws.com',
    'ec2-54-68-40-1.us-west-2.compute.amazonaws.com',
    'ec2-54-68-40-8.us-west-2.compute.amazonaws.com',
    'ec2-54-68-39-242.us-west-2.compute.amazonaws.com',
    'ec2-54-68-38-110.us-west-2.compute.amazonaws.com',
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
