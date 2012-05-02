#! /bin/sh -e

. ./scripts/ec2-common.sh

if [ ! -f "$JAR" ]; then
	echo "file $JAR not found" && exit 1
fi

# TOPOLOGY
C1=$EC2_EU_SERVER1
C2=$EC2_US_CLIENT
CLIENTS="$C1 $C2"
DC1=$EC2_EU_SERVER2
DC2=$EC2_US_SERVER
DCS="$DC1 $DC2"
MACHINES="$CLIENTS $DCS"

# INPUT DATA
FILES_LOCAL_PREFIX=scripts/
USERS_FILE=users.txt
C1_CMDS_FILE=commands.txt
C2_CMDS_FILE=commands.txt
FILES="$USERS_FILE $C1_CMDS_FILE $C2_CMDS_FILE"

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
	swift_app_cmd swift.application.social.SwiftSocialBenchmark $server $ISOLATION $CACHING $CACHE_EVICTION_TIME_MS $NOTIFICATIONS $ASYNC_COMMIT $THINK_TIME_MS $input_file true
	run_cmd $client $CMD
}

# run_swift_client_bg <client> <server> <cmds_file>
run_swift_client_bg() {
	client=$1
	server=$2
	input_file=$3
	swift_app_cmd swift.application.social.SwiftSocialBenchmark $server $ISOLATION $CACHING $CACHE_EVICTION_TIME_MS $NOTIFICATIONS $ASYNC_COMMIT $THINK_TIME_MS $input_file false
	run_cmd_bg $client $CMD
}


echo "deploying swift social test"
if [ -n "$DEPLOY" ]; then
	deploy_swift_on_many $MACHINES
	copy_to $FILES_LOCAL_PREFIX$USERS_FILE $C1 $USERS_FILE
	copy_to $FILES_LOCAL_PREFIX$C1_CMDS_FILE $C1 $C1_CMDS_FILE
	copy_to $FILES_LOCAL_PREFIX$C2_CMDS_FILE $C2 $C2_CMDS_FILE
fi

echo "starting sequencers and DC servers"
./scripts/ec2-start-servers.sh $DCS

echo "waiting a bit before initializing database"
sleep 10

echo "initializing database"
run_swift_client_initdb $C1 $DC1 $USERS_FILE

echo "waiting a bit before starting real clients"
sleep 10

echo "starting client 1"
run_swift_client_bg $C1 $DC1 $C1_CMDS_FILE

echo "starting client 2"
run_swift_client_bg $C2 $DC2 $C2_CMDS_FILE

echo "running ... hit enter when you think its finished"
read dummy

echo "killing servers and clients"
kill_swift $C1 || true
kill_swift $C2 || true
kill_swift $DC1 || true
kill_swift $DC2 || true

echo "collecting client log to result log"
output_prefix=results/result-social-$ISOLATION-$CACHING-$NOTIFICATIONS-$CACHE_EVICTION_TIME_MS-$ASYNC_COMMIT-$THINK_TIME_MS.log
copy_from $C1 "cat stdout.txt" $output_prefix.client1
copy_from $C2 "stdout.txt" $output_prefix.client2
less $output_prefix.client1
