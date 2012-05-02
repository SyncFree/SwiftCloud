#!/bin/bash
# ec2-run <server> <class> <args>
. scripts/ec2-common.sh

if [ -z "$*" ]; then
        echo "ec2-ping.sh <ec2-instance> <main class package and name> <main class arguments>"
        echo "Pings ec2-instance" 
	exit 1
fi

server1=$EC_ASIA_SINGAPORE
server2=$EC_ASIA_TOKYO
PING="ping -n 10 $server2"
run_cmd $server1 $PING > pingtime_$server1_$server2.log

server1=$EC_US_NORTHCALIFORNIA
server2=$EC_US_OREGON
PING="ping -n 10 $server2"
run_cmd $server1 $PING > pingtime_$server1_$server2.log