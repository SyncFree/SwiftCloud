#!/bin/bash
# ec2-deploy <server1 server2 ...>
. scripts/ec2-common.sh

if [ -z "$*" ]; then
  echo "no servers specified on commandline, taking $EC2_ALL"
  servers=$EC2_ALL
else
  servers=$*
fi

if [ ! -f $JAR ]; then
	echo "$JAR not found" && exit 1
fi


for host in $servers; do
	deploy_swift_on $host
done

