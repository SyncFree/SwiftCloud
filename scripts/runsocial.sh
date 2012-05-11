#! /bin/bash

. ./scripts/ec2-common.sh

if [ ! -f "$JAR" ]; then
	echo "file $JAR not found" && exit 1
fi

# TOPOLOGY
DCS[0]=${EC2_TEST_EU[0]}
DCS[1]=${EC2_TEST_EU[1]}
DC_CLIENTS[0]="${EC2_TEST_EU[2]}"
DC_CLIENTS[1]="${EC2_TEST_EU[3]}"
# Use first DC to initialize global users db, to make it fast.
INIT_DB_DC=${DCS[0]}
MACHINES="${DCS[*]} ${DC_CLIENTS[*]}"

# INPUT DATA PARAMS
INPUT_USERS=2000
INPUT_ACTIVE_USERS=50
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
CACHE_EVICTION_TIME_MS=120000
ASYNC_COMMIT=true
THINK_TIME_MS=0
CONCURRENT_SESSIONS_PER_JVM=10

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
	swift_app_cmd_nostdout swift.application.social.SwiftSocialBenchmark run $server commands.txt $ISOLATION $CACHING $CACHE_EVICTION_TIME_MS $NOTIFICATIONS $ASYNC_COMMIT $THINK_TIME_MS $CONCURRENT_SESSIONS_PER_JVM
	run_cmd_bg $client $CMD
}

echo "Preprocessing input parameters"
if [ "${!DCS[*]}" != "${!DC_CLIENTS[*]}" ]; then
	echo "Error: DCs configuration does not match clients configuration"
	exit 1  	
fi

CLIENTS_NUMBER=0
for c in ${DC_CLIENTS[*]}; do
	CLIENTS_NUMBER=$(($CLIENTS_NUMBER+1))
done

echo "Generating input data - generating users db"
mkdir -p input/
scripts/create_users.py 0 $INPUT_USERS $FILE_USERS
echo "Generating input data - generating commands"
scripts/gen_commands_local.py $FILE_USERS $INPUT_USER_FRIENDS $INPUT_USER_BIASED_OPS $INPUT_USER_RANDOM_OPS $INPUT_USER_OPS_GROUPS $CLIENTS_NUMBER $INPUT_ACTIVE_USERS $FILE_CMDS_PREFIX

echo "killing existing servers and clients"
scripts/ec2-kill.sh $MACHINES

echo "deploying swift social test"
if [ -n "$DEPLOY" ]; then
	deploy_swift_on_many $MACHINES
	echo "copying database of users"
	copy_to_bg $FILE_USERS $INIT_DB_DC users.txt
	copy_pids="$!"
	echo "scattering workload definitions to the clients"
	i=0
	for client in ${DC_CLIENTS[*]}; do
		copy_to_bg $FILE_CMDS_PREFIX-$i $client commands.txt
		copy_pids="$copy_pids $!"
		i=$(($i+1))
	done
	echo "awaiting transfers completion"
	wait $copy_pids
fi

echo "starting sequencers and DC servers"
./scripts/ec2-start-servers.sh ${DCS[*]}

echo "waiting a bit before initializing database"
sleep 10

echo "initializing database"
run_swift_client_initdb $INIT_DB_DC $INIT_DB_DC users.txt

echo "waiting a bit before starting real clients"
sleep 10

for i in ${!DCS[*]}; do
	echo "starting clients connecting to DC$i"
	for client in ${DC_CLIENTS[$i]}; do
		run_swift_client_bg "$client" "${DCS[$i]}"
	done	
done

echo "running ... hit enter when you think its finished"
read dummy

echo "killing servers and clients"
scripts/ec2-kill.sh $MACHINES

echo "collecting client log to result log"
output_prefix=results/result-social-$ISOLATION-$CACHING-$NOTIFICATIONS-$CACHE_EVICTION_TIME_MS-$ASYNC_COMMIT-$THINK_TIME_MS.log
for client in ${DC_CLIENTS[*]}; do
	copy_from $client stdout.txt $output_prefix.$client
done

