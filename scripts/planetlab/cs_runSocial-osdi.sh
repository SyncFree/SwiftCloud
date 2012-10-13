#! /bin/bash


. ./scripts/planetlab/pl-common.sh

export DATACENTER_SERVERS=(
planetlab1.di.fct.unl.pt
planetlab2.fct.ualg.pt
)


export SCOUT_NODES=(
planetlab1.fct.ualg.pt
planetlab2.di.fct.unl.pt
)


export ENDCLIENT_NODES=(
planetlab-um00.di.uminho.pt
planetlab-um10.di.uminho.pt
)


# BELOW NOT USED, JUST A POOL OF AVAILABLE PLANETLAB NODES



# WARNING - PlanetLab nodes are volatile; some may be down...
export PLANETLAB_NODES_ALL=(
ait21.us.es
ait05.us.es


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



# TOPOLOGY
DCS[0]=${DATACENTER_SERVERS[0]}
DCSEQ[0]=${DATACENTER_SERVERS[0]}

DCS[1]=${DATACENTER_SERVERS[1]}
DCSEQ[1]=${DATACENTER_SERVERS[1]}

SCOUTS=("${SCOUT_NODES[@]}")

ENDCLIENTS=("${ENDCLIENT_NODES[@]}")

MACHINES="${DCS[*]} ${DCSEQ[*]} ${SCOUTS[*]} ${ENDCLIENTS[*]}"

INIT_DB_DC=${DCS[0]}
INIT_DB_CLIENT=${DCS[1]}

#INIT_DB_DC2=${DCS[1]}
#INIT_DB_CLIENT2=${DCS[0]}

echo $INIT_DB_CLIENT
echo $INIT_DB_CLIENT2

# INPUT DATA PARAMS
INPUT_USERS=1000
INPUT_ACTIVE_USERS=10
INPUT_USER_FRIENDS=25
INPUT_USER_BIASED_OPS=9
INPUT_USER_RANDOM_OPS=1
INPUT_USER_OPS_GROUPS=500
FILE_USERS=input/users.txt
FILE_CMDS_PREFIX=input/commands.txt

# BENCHMARK PARAMS
NOTIFICATIONS=false
ISOLATION=REPEATABLE_READS
CACHING=STRICTLY_MOST_RECENT
CACHING=CACHED
CACHE_EVICTION_TIME_MS=120000 #120000
ASYNC_COMMIT=true
THINK_TIME_MS=0
MAX_CONCURRENT_SESSIONS_PER_JVM=10


DC_NUMBER=${#DCS[@]}
SCOUTS_NUMBER=${#SCOUTS[@]}
CLIENTS_NUMBER=${#ENDCLIENTS[@]}

echo "==== KILLING EXISTING SERVERS AND CLIENTS ===="
. scripts/planetlab/pl-kill.sh $MACHINES
echo "==== DONE ===="

sleep 10

ant -buildfile smd-jar-build.xml 

if [ ! -f "$JAR" ]; then
echo "file $JAR not found" && exit 1
fi

# DEPLOY STUFF?
DEPLOY=true

run_swift_client_initdb() {
client=$1
server=$2
input_file=$3
swift_app_cmd swift.application.social.SwiftSocialBenchmark init $server users.txt
run_cmd $client $CMD
}

# run_swift_client_bg <client> <server> <cmds_file>
run_swift_cdn_server_bg() {
client=$1
server=$2
input_file=$3
swift_app_cmd_nostdout -Xmx512m -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.port=7778 -Djava.rmi.server.hostname=$client swift.application.social.cdn.SwiftSocialBenchmarkServer run $server commands.txt $ISOLATION $CACHING $CACHE_EVICTION_TIME_MS $NOTIFICATIONS $ASYNC_COMMIT $THINK_TIME_MS $MAX_CONCURRENT_SESSIONS_PER_JVM
run_cmd_bg $client $CMD
}

run_swift_cdn_client_bg() {
client=$1
server=$2
input_file=$3
swift_app_cmd_nostdout -Xmx256m -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.port=7779 -Djava.rmi.server.hostname=$client swift.application.social.cdn.SwiftSocialBenchmarkClient $server commands.txt $MAX_CONCURRENT_SESSIONS_PER_JVM
run_cmd_bg $client $CMD
}

echo "==== GENERATING INPUT DATA - GENERATING USERS DB ===="
mkdir -p input/
scripts/planetlab/create_users.py 0 $INPUT_USERS $FILE_USERS
echo "==== GENERATING INPUT DATA - GENERATING COMMANDS ===="
scripts/planetlab/gen_commands_local.py $FILE_USERS $INPUT_USER_FRIENDS $INPUT_USER_BIASED_OPS $INPUT_USER_RANDOM_OPS $INPUT_USER_OPS_GROUPS $CLIENTS_NUMBER $INPUT_ACTIVE_USERS $FILE_CMDS_PREFIX

echo "==== DEPLOYING SWIFTCLOUD BINARIES ===="
if [ -n "$DEPLOY" ]; then
	deploy_swift_on_many $MACHINES
	echo "==== COPYING DB OF USERS ===="
	copy_to_bg $FILE_USERS $INIT_DB_CLIENT users.txt
#    copy_to_bg $FILE_USERS $INIT_DB_CLIENT2 users.txt
	copy_pids="$!"
	echo "==== SCATTERING WORKLOAD DEFINITIONS TO SCOUTS AND CLIENTS ===="
	i=0
	for client in ${SCOUTS[*]}; do
		copy_to_bg $FILE_CMDS_PREFIX-$i $client commands.txt
		i=$(($i+1))
	done
    i=0
    for client in ${ENDCLIENTS[*]}; do
        copy_to_bg $FILE_CMDS_PREFIX-$i $client commands.txt
        i=$(($i+1))
    done

	echo "==== AWAITING TRANSFERS COMPLETION ===="
	echo "PIDS:" + $copy_pids
fi

echo "==== STARTING SEQUENCERS AND DC SERVERS ===="
. scripts/planetlab/pl-start-servers-ds-seq.sh
echo "==== STARTING SEQUENCERS" $DCSEQ "AND DC SERVERS ====" $DCS

servers_start DCS DCSEQ

echo "==== WAITING A BIT BEFORE INITIALIZING DATABASE ===="
sleep 30

echo "==== INITIALIZING DATABASE ===="
echo "CLIENT" + $INIT_DB_CLIENT, "SERVER" + $INIT_DB_DC


run_swift_client_initdb $INIT_DB_CLIENT $INIT_DB_DC users.txt
#run_swift_client_initdb $INIT_DB_CLIENT2 $INIT_DB_DC2 users.txt

echo "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
sleep 30

echo "DCS: " $DC_NUMBER "CLIENTS: " $CLIENTS_NUMBER

scout_pids=""

i=0;
for scout in ${SCOUTS[*]}; do
	j=$(($i % $DC_NUMBER))
	SCOUT_DC=${DCS[$j]}
	echo "==== STARTING CDN SCOUT+SWIFTSOCIALSERVER Nº $i @ $scout CONNECTING TO $SCOUT_DC ===="
		run_swift_cdn_server_bg "$scout" "$SCOUT_DC"
		scout_pids="$scout_pids $!"
		i=$(($i+1))
done

echo "==== WAITING A BIT BEFORE STARTING ENDCLIENTS ===="

sleep 20

client_pids=""
i=0;
for client in ${ENDCLIENTS[*]}; do
    j=$(($i % $SCOUTS_NUMBER))
    CLIENT_SCOUT=${SCOUTS[$j]}
    echo "==== STARTING CDN ENDCLIENT Nº $i @ $client CONNECTING TO $CLIENT_SCOUT ===="
    run_swift_cdn_client_bg "$client" "$CLIENT_SCOUT"
    client_pids="$client_pids $!"
    i=$(($i+1))
done


echo "==== RUNNING... ===="
wait $client_pids

echo "==== WAITING A BIT FOR PENDING OPS ON SERVERS ===="
sleep 60
echo "==== KILLING SERVERS AND CLIENTS ===="
scripts/planetlab/pl-kill.sh $MACHINES

echo "==== COLLECTING CLIENT LOGS AS RESULTS ===="
output_prefix=results/swiftsocial/1pc-osdi-result-cs-social-$ISOLATION-$CACHING-$NOTIFICATIONS-$CACHE_EVICTION_TIME_MS-$ASYNC_COMMIT-$THINK_TIME_MS.log
for client in ${ENDCLIENTS[*]}; do
	copy_from $client stdout.txt $output_prefix.$client
done

