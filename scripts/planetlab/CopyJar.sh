#! /bin/bash


. ./scripts/planetlab/pl-common.sh



# TOPOLOGY
DCS[0]=${EC2_PROD_EU_M1SMALL[2]}
DCS[1]=${EC2_PROD_EU_M1SMALL[3]}
DC_CLIENTS[0]=${EC2_PROD_EU_M1SMALL[@]:0:1}
DC_CLIENTS[1]=${EC2_PROD_EU_M1SMALL[@]:1:1}

MACHINES="${DCS[*]} ${DC_CLIENTS[*]}"
INIT_DB_DC=${DCS[0]}
INIT_DB_CLIENT=${EC2_PROD_EU_M1SMALL[0]}


# INPUT DATA PARAMS
INPUT_USERS=500
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
CACHE_EVICTION_TIME_MS=120 #120000
ASYNC_COMMIT=false
THINK_TIME_MS=0
MAX_CONCURRENT_SESSIONS_PER_JVM=$INPUT_ACTIVE_USERS

echo "==== KILLING EXISTING SERVERS AND CLIENTS ===="
scripts/planetlab/pl-kill.sh $MACHINES
echo "==== DONE ===="

ant -buildfile smd-jar-build.xml 

if [ ! -f "$JAR" ]; then
echo "file $JAR not found" && exit 1
fi

# DEPLOY STUFF?
DEPLOY=true

# run_swift_client_initdb <client> <server> <users_file>
run_swift_client_initdb() {
	client=$1
	server=$2
	input_file=$3
	swift_app_cmd swift.application.social.SwiftSocialBenchmark init $server users.txt
	run_cmd $client $CMD
}

# run_swift_client_bg <client> <server> <cmds_file>
run_swift_client_bg() {
	client=$1
	server=$2
	input_file=$3
	swift_app_cmd_nostdout swift.application.social.SwiftSocialBenchmark run $server commands.txt $ISOLATION $CACHING $CACHE_EVICTION_TIME_MS $NOTIFICATIONS $ASYNC_COMMIT $THINK_TIME_MS $MAX_CONCURRENT_SESSIONS_PER_JVM
	run_cmd_bg $client $CMD
}

echo "==== DEPLOYING SWIFTCLOUD BINARIES ===="
if [ -n "$DEPLOY" ]; then
	deploy_swift_on_many $MACHINES
	echo "==== COPYING DB OF USERS ===="
	copy_to_bg $FILE_USERS $INIT_DB_CLIENT users.txt
	copy_pids="$!"
	echo "==== SCATTERING WORKLOAD DEFINITIONS TO THE CLIENTS ===="
	i=0
	for client in ${DC_CLIENTS[*]}; do
		copy_to_bg $FILE_CMDS_PREFIX-$i $client commands.txt
		copy_pids="$copy_pids $!"
		i=$(($i+1))
	done
	echo "==== AWAITING TRANSFERS COMPLETION ===="
	echo "PIDS:" + $copy_pids
	wait $copy_pids
fi