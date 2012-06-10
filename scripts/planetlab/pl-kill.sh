#!/bin/bash
# ec2-kill <server1 server2 ...>

. scripts/planetlab/pl-common.sh

if [ -z "$*" ]; then
	echo "ec2-kill.sh <ec2-instance1 ec2-instance2 ...>"
	echo "Stops java processes at provided hosts"
	exit 1
fi

if [ "$1" == all ]; then
  servers=$EC2_ALL
else
  servers=$*
fi

# stop <server1 server2 ...>
stop() {
        servers=$*
        echo "Stopping swift apps: $servers"
        for server in $servers; do
                kill_swift $server
        done
}

echo "killing: $servers"
stop $servers
