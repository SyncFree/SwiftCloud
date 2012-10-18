#! /bin/bash

. ./scripts/planetlab/pl-common.sh

export DATACENTER_NODES=(
mars.planetlab.haw-hamburg.de
)


export SCOUT_NODES=(
onelab-1.fhi-fokus.de
planetlab1.eurecom.fr
pl1.uni-rostock.de
)


# BELOW NOT USED, JUST A POOL OF AVAILABLE PLANETLAB NODES

# WARNING - PlanetLab nodes are volatile; some may be down...
export SPARES_PLANETLAB_NODES_ALL=(
planetlab1.eurecom.fr
planetlab2.eurecom.fr
pl1.uni-rostock.de
)

# TOPOLOGY
DCS[0]=${DATACENTER_NODES[0]}
DCSEQ[0]=${DATACENTER_NODES[0]}

SCOUTS=("${SCOUT_NODES[@]}")

ENDCLIENTS=("${ENDCLIENT_NODES[@]}")

MACHINES="${DCS[*]} ${DCSEQ[*]} ${SCOUTS[*]} ${ENDCLIENTS[*]}"

# BENCHMARK PARAMS
NOTIFICATIONS=true
ISOLATION=REPEATABLE_READS
CACHING=STRICTLY_MOST_RECENT
CACHING=CACHED
CACHE_EVICTION_TIME_MS=120000 #120000
ASYNC_COMMIT=false

DC_NUMBER=${#DCS[@]}
SCOUTS_NUMBER=${#SCOUTS[@]}
CLIENTS_NUMBER=${#ENDCLIENTS[@]}


echo "==== KILLING EXISTING SERVERS AND SCOUTS ===="
. scripts/planetlab/pl-kill.sh $MACHINES
echo "==== DONE ===="

if [ ! -f "$JAR" ]; then
echo "file $JAR not found" && exit 1
fi

sleep 10

# DEPLOY STUFF?
DEPLOY=true

# run_swift_client_bg <client> <server> <cmds_file>
run_swift_cdn_server_bg() {
    target=$1
    id=$2
    id=$(($id+1))
    server=$3
    swift_app_cmd_nostdout -Xmx1024m swift.application.filesystem.cs.SwiftFuseServer -server $server $id $ISOLATION $CACHING $NOTIFICATIONS
    run_cmd_bg $target $CMD
}

echo "==== DEPLOYING SWIFTCLOUD BINARIES ===="
if [ -n "$DEPLOY" ]; then
	deploy_swift_on_many $MACHINES
fi

echo "==== STARTING SEQUENCERS AND DC SERVERS ===="
. scripts/planetlab/pl-start-servers-ds-seq.sh
echo "==== STARTING SEQUENCERS" $DCSEQ "AND DC SERVERS ====" $DCS

servers_start DCS DCSEQ

echo "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
sleep 10

echo "DCS: " $DC_NUMBER "SCOUTS: " $SCOUTS_NUMBER

scout_pids=""

i=0;
for scout in ${SCOUTS[*]}; do
	j=$(($i % $DC_NUMBER))
	SCOUT_DC=${DCS[$j]}
	echo "==== STARTING CS SCOUT-SWIFTDOC SERVER Nº $i @ $scout CONNECTING TO $SCOUT_DC ===="
		run_swift_cdn_server_bg "$scout" "$i" "$SCOUT_DC" 
		scout_pids="$scout_pids $!"
		i=$(($i+1))
done
echo "==== YOU CAN MOUNT THE FILESYTEM LOCALLY POINTING TO ONE OF:" "${SCOUTS[*]}"



