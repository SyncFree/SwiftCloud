#!/bin/bash
# ec2-run <server> <class> <args>
. scripts/ec2-common.sh

if [ -z "$*" ]; then
        echo "ec2-run.sh <ec2-instance> <main class package and name> <main class arguments>"
        echo "Runs swift java application on given ec2-instance" 
	exit 1
fi

server=$1
class=$2
shift 2
args=$*
swift_app_cmd $class $args

run_cmd $server $CMD

