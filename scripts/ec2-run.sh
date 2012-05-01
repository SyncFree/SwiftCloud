#!/bin/bash
# ec2-run <server> <class> <args>
. scripts/ec2-common.sh

server=$1
class=$2
shift 2
args=$*
swift_app_cmd $class $args

run_cmd $server $CMD

