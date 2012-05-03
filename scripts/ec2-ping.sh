#!/bin/bash
# ec2-run <server> <class> <args>
. scripts/ec2-common.sh


server1=$EC2_ASIA_SINGAPORE1
server2=$EC2_ASIA_TOKYO
PING="ping -c 10 $server2"
run_cmd "$server1" "$PING" > results/pingtime_"$server1"_"$server2".log

server1=$EC2_US_NORTHCALIFORNIA1
server2=$EC2_US_OREGON
PING="ping -c 10 $server2"
run_cmd "$server1" "$PING" > results/pingtime_"$server1"_"$server2".log

server1=$EC2_US_NORTHCALIFORNIA1
server2=$EC2_ASIA_TOKYO
PING="ping -c 10 $server2"
run_cmd "$server1" "$PING" > results/pingtime_"$server1"_"$server2".log
