#! /bin/sh -e

. ./scripts/ec2-common.sh

if [ ! -f "$JAR" ]; then
	echo "file $JAR not found" && exit 1
fi

C1=$EC2_EU_SERVER1
C2=$EC2_US_CLIENT
DC1=$EC2_EU_SERVER2
DC2=$EC2_US_SERVER

echo "deploying ping test"
if [ -n "$DEPLOY" ]; then
  deploy_swift_on_many $C1 $C2 $DC1 $DC2
fi

echo "starting sequencers and DC servers"
./scripts/ec2-start-servers.sh $DC1 $DC2

echo "waiting a bit before starting clients"
sleep 10

echo "starting clients"
swift_app_cmd swift.application.PingSpeedBenchmark

echo "starting client 1"
run_cmd_bg $C1 $CMD $DC1 10 1 REPEATABLE_READS CACHED false

echo "starting client 2"
run_cmd_bg $C2 $CMD $DC2 10 2 REPEATABLE_READS CACHED false

echo "running ... hit enter when you think its finished"
read dummy

echo "killing servers"
kill_swift $C1 || true
kill_swift $C2 || true
kill_swift $DC1 || true
kill_swift $DC2 || true

echo "collecting client logs"
run_cmd $C1 "cat stdout.txt" > result-runping.log
