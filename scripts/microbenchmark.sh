#!/bin/bash	
. scripts/ec2-common.sh

modes=("CACHED REPEATABLE_READS" "MOST_RECENT SNAPSHOT_ISOLATION")
numWorkers="1 5 10 15 20"
ratios="0 0.2 0.5 1"
maxTxSize="5"
sampleSize="200"
executionTime=$1 #The warm up will be half of this time
numRuns=$2
outputDir='results-micro'

client1=ec2-50-18-43-126.us-west-1.compute.amazonaws.com
client2=ec2-175-41-184-40.ap-southeast-1.compute.amazonaws.com
server1=ec2-50-112-70-85.us-west-2.compute.amazonaws.com
server2=ec2-54-248-18-243.ap-northeast-1.compute.amazonaws.com

DEPLOY=false

echo "deploying microbenchmark"

echo "killing servers"
kill_swift $client1 || true
kill_swift $client2 || true
kill_swift $server1 || true
kill_swift $server2 || true

#if [ -n "$DEPLOY" ]; then
	#deploy_swift_on $client1
	#deploy_swift_on $client2
	#deploy_swift_on $server1
	#deploy_swift_on $server2
#fi

run_cmd $client1 "mkdir $outputDir"
run_cmd $client2 "mkdir $outputDir"




for i in ${!modes[*]}; do
	modeDir=$outputDir/${modes[$i]/ /_}
	run_cmd $client1 "mkdir $modeDir"
	run_cmd $client2 "mkdir $modeDir"
	for ratio_i in $ratios; do
		for workers in $numWorkers; do
			rawDir=$modeDir"/""workers""$workers""_ratio"$ratio_i
			run_cmd $client1 "mkdir $rawDir"
			run_cmd $client2 "mkdir $rawDir"


			
			echo "starting sequencers and DC servers"
			./scripts/ec2-start-servers.sh $server1  $server2
			
			sleep 10
		

			swift_app_cmd swift.test.microbenchmark.SwiftMicroBenchmark

			echo "starting client 1"
			run_cmd_bg $client1 $CMD $sampleSize $maxTxSize $workers $ratio_i $executionTime $numRuns "${modes[$i]}" $server1 $rawDir -p
			echo "starting client 2"
			run_cmd $client2 $CMD $sampleSize $maxTxSize $workers $ratio_i $executionTime $numRuns "${modes[$i]}" $server2 $rawDir -p
		
			sleep $(($1 * $2))

			echo "killing servers"
			kill_swift $client1 || true
			kill_swift $client2 || true
			kill_swift $server1 || true
			kill_swift $server2 || true

			
			echo "collecting client1 log to result log"
			run_cmd $client1 "cat stdout.txt" > results-micro/client1$sampleSize-$maxTxSize-$workers-$ratio_i-$executionTime-$numRuns-mode$i.log			

			echo "collecting client2 log to result log"
			run_cmd $client2 "cat stdout.txt" > results-micro/client2$sampleSize-$maxTxSize-$workers-$ratio_i-$executionTime-$numRuns-mode$i.log		


		done
	done
done



