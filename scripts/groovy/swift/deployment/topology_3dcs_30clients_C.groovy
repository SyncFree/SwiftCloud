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
    'ec2-54-164-153-144.compute-1.amazonaws.com',

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
]

OregonEC2 = [
    // first node is a DC, followed by 10 scouts
    'ec2-54-68-129-156.us-west-2.compute.amazonaws.com',

    'ec2-54-68-8-176.us-west-2.compute.amazonaws.com',
    'ec2-54-68-129-132.us-west-2.compute.amazonaws.com',
    'ec2-54-68-129-152.us-west-2.compute.amazonaws.com',
    'ec2-54-68-127-222.us-west-2.compute.amazonaws.com',
    'ec2-54-68-46-219.us-west-2.compute.amazonaws.com',
    'ec2-54-68-72-35.us-west-2.compute.amazonaws.com',
    'ec2-54-68-95-185.us-west-2.compute.amazonaws.com',
    'ec2-54-68-129-146.us-west-2.compute.amazonaws.com',
    'ec2-54-68-48-134.us-west-2.compute.amazonaws.com',
    'ec2-54-68-19-155.us-west-2.compute.amazonaws.com',
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
