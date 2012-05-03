#! /bin/sh -e

. ./scripts/ec2-common.sh

if [ ! -f "$JAR" ]; then
	echo "file $JAR not found" && exit 1
fi

# TOPOLOGY
DC1=$EC2_ASIA_TOKYO
DC2=$EC2_US_OREGON
DCS="$DC1 $DC2"
DC1_CLIENTS="$EC2_ASIA_SINGAPORE1 $EC2_ASIA_SINGAPORE2"
DC2_CLIENTS="$EC2_US_NORTHCALIFORNIA1 $EC2_US_NORTHCALIFORNIA2"
INIT_CLIENT=$EC2_ASIA_TOKYO
CLIENTS="$DC1_CLIENTS $DC2_CLIENTS"
MACHINES="$CLIENTS $DCS"

# INPUT DATA PARAMS
INPUT_USERS=5000
INPUT_ACTIVE_USERS=100
INPUT_USER_FRIENDS=25
INPUT_USER_BIASED_OPS=9
INPUT_USER_RANDOM_OPS=1
INPUT_USER_OPS_GROUPS=50
FILE_USERS=input/users.txt
FILE_CMDS_PREFIX=input/commands.txt

# BENCHMARK PARAMS
NOTIFICATIONS=true
ISOLATION=SNAPSHOT_ISOLATION
CACHING=CACHED
CACHE_EVICTION_TIME_MS=120000
ASYNC_COMMIT=true
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

INPUT_SITES=0
for c in $CLIENTS; do
	INPUT_SITES=$(($INPUT_SITES+1))
done


echo "Generating input data - generating users db"
mkdir -p input/
scripts/create_users.py 0 $INPUT_USERS $FILE_USERS
echo "Generating input data - generating commands"
scripts/gen_commands_local.py $FILE_USERS $INPUT_USER_FRIENDS $INPUT_USER_BIASED_OPS $INPUT_USER_RANDOM_OPS $INPUT_USER_OPS_GROUPS $INPUT_SITES $INPUT_ACTIVE_USERS $FILE_CMDS_PREFIX

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
sleep 100

echo "starting clients connecting to DC1"
for client in $DC1_CLIENTS; do
	run_swift_client_bg $client $DC1
done

echo "starting clients connecting to DC2"
for client in $DC2_CLIENTS; do
	run_swift_client_bg $client $DC2
done

echo "starting clients connecting to DC3"
for client in $DC3_CLIENTS; do
	run_swift_client_bg $client $DC3
done

echo "starting clients connecting to DC4"
for client in $DC4_CLIENTS; do
	run_swift_client_bg $client $DC4
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

