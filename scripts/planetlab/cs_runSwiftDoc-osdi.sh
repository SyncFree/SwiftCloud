#! /bin/bash

. ./scripts/planetlab/pl-common.sh

export DATACENTER_NODES=(
ec2-79-125-56-10.eu-west-1.compute.amazonaws.com
)


export SCOUT_NODES=(
planetlab-1.iscte.pt
planetlab-2.iscte.pt
)


export ENDCLIENT_NODES=(
planetlab-3.iscte.pt
planetlab-4.iscte.pt
)

# BELOW NOT USED, JUST A POOL OF AVAILABLE PLANETLAB NODES

# WARNING - PlanetLab nodes are volatile; some may be down...
export PLANETLAB_NODES_ALL=(
ait21.us.es
ait05.us.es

planetlab-um00.di.uminho.pt
planetlab-um10.di.uminho.pt

planetlab2.di.fct.unl.pt
planetlab-1.iscte.pt

ple2.ipv6.lip6.fr
peeramide.irisa.fr
planetlab-2.imag.fr
planetlab1.fct.ualg.pt
planetlab2.fct.ualg.pt
planetlab-um10.di.uminho.pt
planetlab-um00.di.uminho.pt
planetlab1.fct.ualg.pt
planetlab1.di.fct.unl.pt
planetlab2.di.fct.unl.pt
planetlab-1.tagus.ist.utl.pt
planetlab-2.tagus.ist.utl.pt
planetlab-1.tagus.ist.utl.pt
planetlab-2.tagus.ist.utl.pt
planetlab1.eurecom.fr
planetlab2.eurecom.fr

)

# TEST instances
export EC2_TEST_EU=(
)

# TOPOLOGY
DCS[0]=${DATACENTER_NODES[0]}
DCSEQ[0]=${DATACENTER_NODES[0]}

#DCS[1]=${EC2_PROD_EU_MICRO[1]}
#DCSEQ[1]=${EC2_PROD_EU_MICRO[1]}

SCOUTS=("${SCOUT_NODES[@]}")

ENDCLIENTS=("${ENDCLIENT_NODES[@]}")

MACHINES="${DCS[*]} ${DCSEQ[*]} ${SCOUTS[*]} ${ENDCLIENTS[*]}"

# BENCHMARK PARAMS
NOTIFICATIONS=true
ISOLATION=REPEATABLE_READS
CACHING=STRICTLY_MOST_RECENT
CACHING=CACHED
CACHE_EVICTION_TIME_MS=120000 #120000

DC_NUMBER=${#DCS[@]}
SCOUTS_NUMBER=${#SCOUTS[@]}
CLIENTS_NUMBER=${#ENDCLIENTS[@]}

echo "==== KILLING EXISTING SERVERS AND CLIENTS ===="
. scripts/planetlab/pl-kill.sh $MACHINES
echo "==== DONE ===="

ant -buildfile smd-jar-build.xml 

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
    swift_app_cmd_nostdout -Xmx1024m swift.application.swiftdoc.cs.SwiftDocBenchmarkServer $server $id $ISOLATION $CACHING $NOTIFICATIONS
    run_cmd_bg $target $CMD
}

run_swift_cdn_client_bg() {
    target=$1
    id=$2
    id=$(($id+1))
    server=$3
    dcname=$4
    swift_app_cmd_nostdout -Xmx1024m swift.application.swiftdoc.cs.SwiftDocBenchmarkClient $server $id $dcname

    run_cmd_bg $target $CMD
}

echo "==== DEPLOYING SWIFTCLOUD BINARIES ===="
if [ -n "$DEPLOY" ]; then
	deploy_swift_on_many $MACHINES

    echo "==== SCATTERING DIFF PATCHES TO CLIENTS ===="
    i=0
    for client in ${ENDCLIENTS[*]}; do
        copy_to_bg  data/swiftdoc/swiftdoc-patches.zip $client swiftdoc-patches.zip
        i=$(($i+1))
    done
	echo "==== AWAITING TRANSFERS COMPLETION ===="
fi

echo "==== STARTING SEQUENCERS AND DC SERVERS ===="
. scripts/planetlab/pl-start-servers-ds-seq.sh
echo "==== STARTING SEQUENCERS" $DCSEQ "AND DC SERVERS ====" $DCS

servers_start DCS DCSEQ

echo "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
sleep 10

echo "DCS: " $DC_NUMBER "CLIENTS: " $CLIENTS_NUMBER

scout_pids=""

i=0;
for scout in ${SCOUTS[*]}; do
	j=$(($i % $DC_NUMBER))
	SCOUT_DC=${DCS[$j]}
	echo "==== STARTING CDN SCOUT-SWIFTDOC SERVER Nº $i @ $scout CONNECTING TO $SCOUT_DC ===="
		run_swift_cdn_server_bg "$scout" "$i" "$SCOUT_DC" 
		scout_pids="$scout_pids $!"
		i=$(($i+1))
done
echo "==== WAITING A BIT BEFORE STARTING ENDCLIENTS ===="

sleep 20

client_pids=()
i=0;
for client in ${ENDCLIENTS[*]}; do
    j=$(($i % $SCOUTS_NUMBER))
    CLIENT_SCOUT=${SCOUTS[$j]}
    k=$(($j % $DC_NUMBER))
    SCOUT_DC=${DCS[$k]}
    echo "==== STARTING CDN ENDCLIENT Nº $i @ $client CONNECTING TO $CLIENT_SCOUT ===="
    run_swift_cdn_client_bg "$client" "$i" "$CLIENT_SCOUT" "$SCOUT_DC"
    client_pids[$i]="$!"
    i=$(($i+1))
done

echo "==== RUNNING... ===="
wait "${client_pids[0]}"
echo "==== KILLING SERVERS AND CLIENTS ===="
. scripts/planetlab/pl-kill.sh $MACHINES

echo "==== COLLECTING CLIENT LOGS AS RESULTS ===="
output_prefix=results/swiftdoc/1pc-osdi-result-cs-swiftdoc-$ISOLATION-$CACHING-$NOTIFICATIONS.log
for client in ${ENDCLIENTS[*]}; do
	copy_from $client stdout.txt $output_prefix.$client
done

