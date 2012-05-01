#!/bin/bash
# ec2-kill <server1 server2 ...>
. scripts/ec2-common.sh

# stop <server1 server2 ...>
stop() {
        servers=$*
        echo "Stopping swift apps: $servers"
        for server in $servers; do
                kill_swift $server
        done
}

stop $*

