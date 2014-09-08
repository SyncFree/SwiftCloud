#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-77-116-181.eu-west-1.compute.amazonaws.com',
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

    'ec2-54-165-121-170.compute-1.amazonaws.com',
    'ec2-54-165-63-190.compute-1.amazonaws.com',
    'ec2-54-164-153-144.compute-1.amazonaws.com',
    'ec2-54-165-83-13.compute-1.amazonaws.com',
    'ec2-54-165-82-168.compute-1.amazonaws.com',
    'ec2-54-165-83-240.compute-1.amazonaws.com',
    'ec2-54-165-84-39.compute-1.amazonaws.com',
    'ec2-54-165-85-112.compute-1.amazonaws.com',
    'ec2-54-165-86-237.compute-1.amazonaws.com',
    'ec2-54-165-85-26.compute-1.amazonaws.com',
]


// TOPOLOGY
Topology.clear()

DC_EU = DC([EuropeEC2[0]], [EuropeEC2[0]])

ScoutsToEU = SGroup(NVirginiaEC2, DC_EU)
