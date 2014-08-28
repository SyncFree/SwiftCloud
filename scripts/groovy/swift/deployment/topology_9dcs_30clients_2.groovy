#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

// NODES
EuropeEC2 = [
    // DC only
    'ec2-54-72-14-141.eu-west-1.compute.amazonaws.com'
    // TODO DC
    // TODO DC
]

NVirginiaEC2 = [
    // first node is a DC, followed by two groups of 10 scouts, and 2 DCs
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
    'ec2-54-164-171-206.compute-1.amazonaws.com',
    // TODO: DC
    // TODO DC
]

OregonEC2 = [
    // first node is a DC, followed by 10 scouts, and 2 DCs
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
    // TODO: DC
    // TODO: DC
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
