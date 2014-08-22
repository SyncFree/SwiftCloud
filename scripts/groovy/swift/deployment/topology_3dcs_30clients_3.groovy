#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-77-84-48.eu-west-1.compute.amazonaws.com'
]

NVirginiaEC2 = [
    // first node is a DC, followed by two groups of 10 scouts
    'ec2-54-164-174-58.compute-1.amazonaws.com',

    'ec2-54-164-196-180.compute-1.amazonaws.com',
    'ec2-54-164-196-216.compute-1.amazonaws.com',
    'ec2-54-164-197-211.compute-1.amazonaws.com',
    'ec2-54-164-196-210.compute-1.amazonaws.com',
    'ec2-54-164-196-204.compute-1.amazonaws.com',
    'ec2-54-164-196-208.compute-1.amazonaws.com',
    'ec2-54-164-197-7.compute-1.amazonaws.com',
    'ec2-54-164-57-244.compute-1.amazonaws.com',
    'ec2-54-164-198-33.compute-1.amazonaws.com',
    'ec2-54-164-64-148.compute-1.amazonaws.com',
    
    'ec2-54-164-98-77.compute-1.amazonaws.com',
    'ec2-54-164-199-36.compute-1.amazonaws.com',
    'ec2-54-164-199-41.compute-1.amazonaws.com',
    'ec2-54-164-199-40.compute-1.amazonaws.com',
    'ec2-54-164-199-38.compute-1.amazonaws.com',
    'ec2-54-164-199-35.compute-1.amazonaws.com',
    'ec2-54-164-199-33.compute-1.amazonaws.com',
    'ec2-54-164-199-32.compute-1.amazonaws.com',
    'ec2-54-164-199-34.compute-1.amazonaws.com',
    'ec2-54-164-199-39.compute-1.amazonaws.com',
]

OregonEC2 = [
    // first node is a DC, followed by 10 scouts
    'ec2-54-218-21-89.us-west-2.compute.amazonaws.com',
    
    'ec2-54-218-22-241.us-west-2.compute.amazonaws.com',
    'ec2-54-218-22-239.us-west-2.compute.amazonaws.com',
    'ec2-54-218-22-232.us-west-2.compute.amazonaws.com',
    'ec2-54-218-22-240.us-west-2.compute.amazonaws.com',
    'ec2-54-201-151-156.us-west-2.compute.amazonaws.com',
    'ec2-54-218-22-250.us-west-2.compute.amazonaws.com',
    'ec2-54-218-22-242.us-west-2.compute.amazonaws.com',
    'ec2-54-218-22-233.us-west-2.compute.amazonaws.com',
    'ec2-54-218-22-243.us-west-2.compute.amazonaws.com',
    'ec2-54-218-22-251.us-west-2.compute.amazonaws.com',
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
