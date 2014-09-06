#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-77-56-23.eu-west-1.compute.amazonaws.com',
]

NVirginiaEC2 = [
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

'ec2-54-165-65-151.compute-1.amazonaws.com',
'ec2-54-165-65-80.compute-1.amazonaws.com',
'ec2-54-165-70-63.compute-1.amazonaws.com',
'ec2-54-84-139-143.compute-1.amazonaws.com',
'ec2-54-165-20-214.compute-1.amazonaws.com',
'ec2-54-165-31-202.compute-1.amazonaws.com',
'ec2-54-165-71-188.compute-1.amazonaws.com',
'ec2-54-165-57-169.compute-1.amazonaws.com',
'ec2-54-165-64-120.compute-1.amazonaws.com',
'ec2-54-164-5-227.compute-1.amazonaws.com',
]

// TOPOLOGY
Topology.clear()

DC_EU = DC([EuropeEC2[0]], [EuropeEC2[0]])

ScoutsToEU = SGroup(NVirginiaEC2, DC_EU)
