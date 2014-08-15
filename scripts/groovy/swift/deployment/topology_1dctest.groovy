#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

Topology.clear()
SGroup(['ec2-54-191-203-95.us-west-2.compute.amazonaws.com',
        'ec2-54-191-191-178.us-west-2.compute.amazonaws.com',
        'ec2-54-191-163-242.us-west-2.compute.amazonaws.com'
        ], DC(['ec2-54-191-192-105.us-west-2.compute.amazonaws.com'],
                ['ec2-54-191-192-105.us-west-2.compute.amazonaws.com']))
