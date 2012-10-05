#!/bin/bash

. ./scripts/planetlab/pl-common.sh

if [ ! -f "$JAR" ]; then
	echo "file $JAR not found" && exit 1
fi

# TOPOLOGY
DCS[0]=${EC2_PROD_EU_MICRO[0]}
DCSEQ[0]=${EC2_PROD_EU_MICRO[0]}

DCS[1]=${EC2_PROD_EU_MICRO[1]}
DCSEQ[1]=${EC2_PROD_EU_MICRO[1]}

DC_CLIENTS=("${PLANETLAB_NODES[@]}")

MACHINES="${DCS[*]} ${DCSEQ[*]} ${DC_CLIENTS[*]}"



DEPLOY=true

echo "==== KILLING EXISTING SERVERS AND CLIENTS ===="
. ./scripts/planetlab/pl-kill.sh $MACHINES
echo "==== DONE ===="

sleep 10

ant -buildfile smd-jar-build.xml 

if [ ! -f "$JAR" ]; then
echo "file $JAR not found" && exit 1
fi

echo "==== DEPLOYING SWIFTCLOUD BINARIES ===="
if [ -n "$DEPLOY" ]; then
	deploy_swift_on_many $MACHINES
fi

echo "==== STARTING SEQUENCERS AND DC SERVERS ===="
. ./scripts/planetlab/pl-start-servers-ds-seq.sh 
servers_start DCS DCSEQ

echo "waiting a bit before starting clients"
sleep 10

echo "starting clients"
swift_app_cmd -Xmx1000m swift.application.swiftdoc.SwiftDocBenchmark

NOTIFICATIONS=true
ISOLATION=REPEATABLE_READS
CACHING=CACHED
ITERATIONS=1

C1=${PLANETLAB_NODES[0]}
C2=${PLANETLAB_NODES[1]}

DC1=${DCS[0]}
DC2=${DCS[0]}


echo "starting client 1"
run_cmd_bg $C1 $CMD $DC1 $ITERATIONS 1 $ISOLATION $CACHING $NOTIFICATIONS

echo "starting client 2"
run_cmd_bg $C2 $CMD $DC1 $ITERATIONS 2 $ISOLATION $CACHING $NOTIFICATIONS


echo "running ... hit enter when you think its finished"
read dummy

echo "killing servers"
echo "==== KILLING SERVERS AND CLIENTS ===="
. ./scripts/planetlab/pl-kill.sh $MACHINES

#echo "collecting client log to result log"
#run_cmd $C1 "cat stdout.txt" > results/result-ping-$ISOLATION-$CACHING-$NOTIFICATIONS.log
#less results/result-ping-$ISOLATION-$CACHING-$NOTIFICATIONS.log
