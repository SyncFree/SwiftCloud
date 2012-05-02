#! /bin/sh -e

. ./scripts/ec2-common.sh

if [ ! -f "$JAR" ]; then
	echo "file $JAR not found" && exit 1
fi

# TOPOLOGY
DC1=$EC2_ASIA_TOKYO
DC2=$EC2_US_OREGON
DCS="$DC1 $DC2"
DC1_CLIENTS=$EC2_ASIA_SINGAPORE
DC2_CLIENTS=$EC2_US_NORTHCALIFORNIA
INIT_CLIENT=$EC2_ASIA_SINGAPORE
CLIENTS="$DC1_CLIENTS $DC2_CLIENTS"
MACHINES="$CLIENTS $DCS"

# INPUT DATA PARAMS
INPUT_USERS=150
INPUT_SITES=2
INPUT_USER_FRIENDS=20
INPUT_USER_BIASED_OPS=9
INPUT_USER_RANDOM_OPS=1
INPUT_USER_OPS_GROUPS=50
FILE_USERS=input/users.txt
FILE_CMDS_PREFIX=input/commands.txt

# BENCHMARK PARAMS
NOTIFICATIONS=false
ISOLATION=REPEATABLE_READS
CACHING=STRICTLY_MOST_RECENT
CACHE_EVICTION_TIME_MS=60000
ASYNC_COMMIT=false
THINK_TIME_MS=1000

# DEPLOY STUFF?
DEPLOY=true

# run_swift_client_initdb <client> <server> <users_file>
run_swift_client_initdb() {
	client=$1
	server=$2
	input_file=$3
	swift_app_cmd swift.application.social.SwiftSocialBenchmark $server $ISOLATION $CACHING $CACHE_EVICTION_TIME_MS $NOTIFICATIONS $ASYNC_COMMIT $THINK_TIME_MS users.txt true
	run_cmd $client $CMD
}

# run_swift_client_bg <client> <server> <cmds_file>
run_swift_client_bg() {
	client=$1
	server=$2
	input_file=$3
	swift_app_cmd_nostdout swift.application.social.SwiftSocialBenchmark $server $ISOLATION $CACHING $CACHE_EVICTION_TIME_MS $NOTIFICATIONS $ASYNC_COMMIT $THINK_TIME_MS commands.txt false
	run_cmd_bg $client $CMD
}

echo "Generating input data - generating users db"
scripts/create_users.py 0 $INPUT_USERS $FILE_USERS
echo "Generating input data - generating commands"
scripts/gen_commands_local.py $FILE_USERS $INPUT_USER_FRIENDS $INPUT_USER_BIASED_OPS $INPUT_USER_RANDOM_OPS $INPUT_USER_OPS_GROUPS $INPUT_SITES $FILE_CMDS_PREFIX

echo "deploying swift social test"
if [ -n "$DEPLOY" ]; then
	deploy_swift_on_many $MACHINES
	copy_to $FILE_USERS $INIT_CLIENT users.txt
	i=0
	for client in $CLIENTS; do
		copy_to $FILE_CMDS_PREFIX-$i $client commands.txt
		i=$(($i+1))
	done
fi

echo "starting sequencers and DC servers"
./scripts/ec2-start-servers.sh $DCS

echo "waiting a bit before initializing database"
sleep 10

echo "initializing database"
run_swift_client_initdb $INIT_CLIENT $DC1 users.txt

echo "waiting a bit before starting real clients"
sleep 10

echo "starting clients connecting to DC1"
for client in $DC1_CLIENTS; do
	run_swift_client_bg $client $DC1
done

echo "starting clients connecting to DC2"
for client in $DC2_CLIENTS; do
	run_swift_client_bg $client $DC2
done

echo "running ... hit enter when you think its finished"
read dummy

echo "killing servers and clients"
for host in $MACHINES; do
	kill_swift $host || true
done

echo "collecting client log to result log"
output_prefix=results/result-social-$ISOLATION-$CACHING-$NOTIFICATIONS-$CACHE_EVICTION_TIME_MS-$ASYNC_COMMIT-$THINK_TIME_MS.log
for client in $CLIENTS; do
	copy_from $client stdout.txt $output_prefix.$client
done

