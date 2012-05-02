#! /bin/sh -e

. ./scripts/ec2-common.sh

if [ ! -f "$JAR" ]; then
	echo "file $JAR not found" && exit 1
fi

C1=$EC2_ASIA_SINGAPORE
C2=$EC2_US_OREGON
DC1=$EC2_ASIA_TOKYO
DC2=$EC2_US_NORTHCALIFORNIA

DEPLOY=true

echo "deploying ping test"
if [ -n "$DEPLOY" ]; then
  deploy_swift_on $C1
  deploy_swift_on $C2
  deploy_swift_on $DC1
  deploy_swift_on $DC2
fi

echo "starting sequencers and DC servers"
./scripts/ec2-start-servers.sh $DC1 $DC2

echo "waiting a bit before starting clients"
sleep 10

echo "starting clients"
swift_app_cmd swift.application.PingSpeedBenchmark

NOTIFICATIONS=false
ISOLATION=REPEATABLE_READS
CACHING=STRICTLY_MOST_RECENT
ITERATIONS=100

echo "starting client 1"
run_cmd_bg $C1 $CMD $DC1 $ITERATIONS 1 $ISOLATION $CACHING $NOTIFICATIONS

echo "starting client 2"
run_cmd_bg $C2 $CMD $DC1 $ITERATIONS 2 $ISOLATION $CACHING $NOTIFICATIONS

echo "running ... hit enter when you think its finished"
read dummy

echo "killing servers"
kill_swift $C1 || true
kill_swift $C2 || true
kill_swift $DC1 || true
kill_swift $DC2 || true

echo "collecting client log to result log"
run_cmd $C1 "cat stdout.txt" > results/result-ping-$ISOLATION-$CACHING-$NOTIFICATIONS.log
less results/result-ping-$ISOLATION-$CACHING-$NOTIFICATIONS.log