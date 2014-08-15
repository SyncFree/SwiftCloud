#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-77-125-192.eu-west-1.compute.amazonaws.com'
]

NVirginiaEC2 = [
    // first node is a DC, followed by two groups of 6 and 7 scouts
    'ec2-54-210-105-159.compute-1.amazonaws.com',
    'ec2-107-23-8-93.compute-1.amazonaws.com',
    'ec2-54-210-180-207.compute-1.amazonaws.com',
    'ec2-54-236-226-56.compute-1.amazonaws.com',
    'ec2-107-23-8-117.compute-1.amazonaws.com',
    'ec2-54-210-232-97.compute-1.amazonaws.com',
    'ec2-54-88-244-46.compute-1.amazonaws.com',
    'ec2-54-88-82-117.compute-1.amazonaws.com',
    'ec2-107-23-7-145.compute-1.amazonaws.com',
    'ec2-107-23-7-176.compute-1.amazonaws.com',
    'ec2-54-84-228-145.compute-1.amazonaws.com',
    'ec2-107-23-8-116.compute-1.amazonaws.com',
    'ec2-107-23-8-110.compute-1.amazonaws.com',
    'ec2-54-210-95-27.compute-1.amazonaws.com'
]

OregonEC2 = [
    // first node is a DC, followed by 7 scouts
    'ec2-54-191-105-195.us-west-2.compute.amazonaws.com',
    'ec2-54-201-30-239.us-west-2.compute.amazonaws.com',
    'ec2-54-191-249-227.us-west-2.compute.amazonaws.com',
    'ec2-54-200-227-7.us-west-2.compute.amazonaws.com',
    'ec2-54-200-161-173.us-west-2.compute.amazonaws.com',
    'ec2-54-200-94-27.us-west-2.compute.amazonaws.com',
    'ec2-54-187-61-206.us-west-2.compute.amazonaws.com',
    'ec2-54-201-18-167.us-west-2.compute.amazonaws.com'
]

// TOPOLOGY
Topology.clear()

DC_EU = DC([EuropeEC2[0]], [EuropeEC2[0]])
DC_NV = DC([NVirginiaEC2[0]], [NVirginiaEC2[0]])
DC_OR = DC([OregonEC2[0]], [OregonEC2[0]])

ScoutsToEU = SGroup(NVirginiaEC2[1..6], DC_EU)
ScoutsToNV = SGroup(OregonEC2[1..7], DC_NV)
ScoutsToOR = SGroup(NVirginiaEC2[7..13], DC_OR)
