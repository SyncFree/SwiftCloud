#!/bin/bash
# ec2-kill <server1 server2 ...>

. scripts/ec2-common.sh

if [ -z "$*" ]; then
	echo "ec2-kill.sh <ec2-instance1 ec2-instance2 ...>"
	echo "Stops java processes at provided hosts"
	exit 1
fi

# stop <server1 server2 ...>
stop() {
        servers=$*
        echo "Stopping swift apps: $servers"
        for server in $servers; do
                kill_swift $server
        done
}

if [ -z "$*" ]; then
  echo "no servers specified on commandline, killing $EC2_ALL"
  servers=$EC2_ALL
else
  servers=$*
fi

stop $servers
