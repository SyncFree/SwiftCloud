#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-76-188-118.eu-west-1.compute.amazonaws.com'
]

NVirginiaEC2 = [
    // first node is a DC, followed by two groups of 6 and 7 scouts
    'ec2-107-23-34-74.compute-1.amazonaws.com',
    'ec2-54-210-190-244.compute-1.amazonaws.com',
    'ec2-107-23-34-114.compute-1.amazonaws.com',
    'ec2-107-23-35-152.compute-1.amazonaws.com',
    'ec2-107-23-34-194.compute-1.amazonaws.com',
    'ec2-54-210-189-179.compute-1.amazonaws.com',
    'ec2-107-21-6-172.compute-1.amazonaws.com',
    'ec2-107-23-35-19.compute-1.amazonaws.com',
    'ec2-107-23-4-205.compute-1.amazonaws.com',
    'ec2-107-23-42-21.compute-1.amazonaws.com',
    'ec2-54-210-190-61.compute-1.amazonaws.com',
    'ec2-107-23-42-113.compute-1.amazonaws.com',
    'ec2-54-236-249-84.compute-1.amazonaws.com',
    'ec2-54-210-248-126.compute-1.amazonaws.com'
]

OregonEC2 = [
    // first node is a DC, followed by 7 scouts
    'ec2-54-201-151-156.us-west-2.compute.amazonaws.com',
    'ec2-54-201-151-159.us-west-2.compute.amazonaws.com',
    'ec2-54-201-151-204.us-west-2.compute.amazonaws.com',
    'ec2-54-201-151-196.us-west-2.compute.amazonaws.com',
    'ec2-54-201-151-221.us-west-2.compute.amazonaws.com',
    'ec2-54-201-151-220.us-west-2.compute.amazonaws.com',
    'ec2-54-201-151-166.us-west-2.compute.amazonaws.com',
    'ec2-54-201-151-173.us-west-2.compute.amazonaws.com'
]

// TOPOLOGY
Topology.clear()

DC_EU = DC([EuropeEC2[0]], [EuropeEC2[0]])
DC_NV = DC([NVirginiaEC2[0]], [NVirginiaEC2[0]])
DC_OR = DC([OregonEC2[0]], [OregonEC2[0]])

ScoutsToEU = SGroup(NVirginiaEC2[1..6], DC_EU)
ScoutsToNV = SGroup(OregonEC2[1..7], DC_NV)
ScoutsToOR = SGroup(NVirginiaEC2[7..13], DC_OR)
