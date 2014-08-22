#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-72-14-141.eu-west-1.compute.amazonaws.com'
]

NVirginiaEC2 = [
    // first node is a DC, followed by two groups of 10 scouts
    'ec2-54-164-74-228.compute-1.amazonaws.com',

    'ec2-54-164-141-234.compute-1.amazonaws.com',
    'ec2-54-164-122-75.compute-1.amazonaws.com',
    'ec2-54-164-83-187.compute-1.amazonaws.com',
    'ec2-54-164-75-64.compute-1.amazonaws.com',
    'ec2-54-164-73-140.compute-1.amazonaws.com',
    'ec2-54-164-130-135.compute-1.amazonaws.com',
    'ec2-54-164-111-249.compute-1.amazonaws.com',
    'ec2-54-164-37-135.compute-1.amazonaws.com',
    'ec2-54-164-42-207.compute-1.amazonaws.com',
    'ec2-54-164-140-191.compute-1.amazonaws.com',

    'ec2-54-164-173-186.compute-1.amazonaws.com',
    'ec2-54-164-172-33.compute-1.amazonaws.com',
    'ec2-54-164-167-121.compute-1.amazonaws.com',
    'ec2-54-164-109-163.compute-1.amazonaws.com',
    'ec2-54-164-174-86.compute-1.amazonaws.com',
    'ec2-54-164-168-251.compute-1.amazonaws.com',
    'ec2-54-164-173-24.compute-1.amazonaws.com',
    'ec2-54-164-171-159.compute-1.amazonaws.com',
    'ec2-54-164-11-60.compute-1.amazonaws.com',
    'ec2-54-164-171-206.compute-1.amazonaws.com'
]

OregonEC2 = [
    // first node is a DC, followed by 10 scouts
    'ec2-54-200-145-124.us-west-2.compute.amazonaws.com',
    
    'ec2-54-187-125-31.us-west-2.compute.amazonaws.com',
    'ec2-54-186-252-221.us-west-2.compute.amazonaws.com',
    'ec2-54-213-229-157.us-west-2.compute.amazonaws.com',
    'ec2-54-213-112-51.us-west-2.compute.amazonaws.com',
    'ec2-54-191-99-244.us-west-2.compute.amazonaws.com',
    'ec2-54-213-228-169.us-west-2.compute.amazonaws.com',
    'ec2-54-201-173-34.us-west-2.compute.amazonaws.com',
    'ec2-54-200-231-210.us-west-2.compute.amazonaws.com',
    'ec2-54-213-229-158.us-west-2.compute.amazonaws.com',
    'ec2-54-213-228-226.us-west-2.compute.amazonaws.com',
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
