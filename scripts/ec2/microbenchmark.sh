#!/bin/bash	
. scripts/ec2-common.sh

modes=("MOST_RECENT SNAPSHOT_ISOLATION")
#numWorkers="1 5 10 15 20"
numWorkers="1 15 30 45 60"
ratios="0 0.2"
maxTxSize="5"
sampleSize="10000"
cltSize="100"
executionTime=$1 #The warm up will be half of this time
numRuns=$2
outputDir='results-micro'

client1=ec2-50-18-43-126.us-west-1.compute.amazonaws.com
#client2=ec2-184-72-23-218.us-west-1.compute.amazonaws.com
#client3=ec2-184-169-229-128.us-west-1.compute.amazonaws.com

client4=ec2-175-41-184-40.ap-southeast-1.compute.amazonaws.com
#client5=ec2-122-248-226-204.ap-southeast-1.compute.amazonaws.com
#client6=ec2-122-248-196-57.ap-southeast-1.compute.amazonaws.com

server1=ec2-50-112-70-85.us-west-2.compute.amazonaws.com
server2=ec2-54-248-18-243.ap-northeast-1.compute.amazonaws.com

DEPLOY=$DEPLOY

echo "deploying microbenchmark"

echo "killing servers"
kill_swift $client1 || true
#kill_swift $client2 || true
#kill_swift $client3 || true
kill_swift $client4 || true
#kill_swift $client5 || true
#kill_swift $client6 || true
kill_swift $server1 || true
kill_swift $server2 || true

if [ -n "$DEPLOY" ]; then
	deploy_swift_on $server1
	deploy_swift_on $server2
fi

run_cmd $client1 "mkdir $outputDir"
#run_cmd $client2 "mkdir $outputDir"
#run_cmd $client3 "mkdir $outputDir"
run_cmd $client4 "mkdir $outputDir"
#run_cmd $client5 "mkdir $outputDir"
#run_cmd $client6 "mkdir $outputDir"

mkdir -p $outputDir

for i in ${!modes[*]}; do
	modeDir=$outputDir/${modes[$i]/ /_}
#	run_cmd $client1 "mkdir $modeDir"
#	run_cmd $client2 "mkdir $modeDir"
	for ratio_i in $ratios; do
		for workers in $numWorkers; do
			rawDir=$modeDir"/""workers""$workers""_ratio"$ratio_i
			run_cmd $client1 "mkdir -p $rawDir"
			#run_cmd $client2 "mkdir -p $rawDir"
			#run_cmd $client3 "mkdir -p $rawDir"
			run_cmd $client4 "mkdir -p $rawDir"
			#run_cmd $client5 "mkdir -p $rawDir"
			#run_cmd $client6 "mkdir -p $rawDir"


			
			echo "starting sequencers and DC servers"
			./scripts/ec2-start-servers.sh $server1  $server2
			
			sleep 10
		

			swift_app_cmd swift.test.microbenchmark.SwiftMicroBenchmark

			run_cmd $server1 $CMD $sampleSize $cltSize $maxTxSize $workers $ratio_i $executionTime $numRuns "${modes[$i]}" $server1 $rawDir -p
			sleep 20
			echo "starting client 1"
			run_cmd_bg $client1 $CMD $sampleSize $cltSize $maxTxSize $workers $ratio_i $executionTime $numRuns "${modes[$i]}" $server1 $rawDir

			#echo "starting client 2"
			#run_cmd_bg $client2 $CMD $sampleSize $cltSize $maxTxSize $workers $ratio_i $executionTime $numRuns "${modes[$i]}" $server1 $rawDir

			#echo "starting client 3"
			#run_cmd_bg $client3 $CMD $sampleSize $cltSize $maxTxSize $workers $ratio_i $executionTime $numRuns "${modes[$i]}" $server1 $rawDir

			echo "starting client 4"
			run_cmd $client4 $CMD $sampleSize $cltSize $maxTxSize $workers $ratio_i $executionTime $numRuns "${modes[$i]}" $server2 $rawDir

			#echo "starting client 5"
			#run_cmd_bg $client5 $CMD $sampleSize $cltSize $maxTxSize $workers $ratio_i $executionTime $numRuns "${modes[$i]}" $server2 $rawDir

			#echo "starting client 6"
			#run_cmd $client6 $CMD $sampleSize $cltSize $maxTxSize $workers $ratio_i $executionTime $numRuns "${modes[$i]}" $server2 $rawDir
		
			sleep $(($1 * $2))

			echo "killing servers"
			kill_swift $server1 || true
			kill_swift $server2 || true

			
			echo "collecting client1 log to result log"
			run_cmd $client1 "cat stdout.txt" > results-micro/client1$sampleSize-$maxTxSize-$workers-$ratio_i-$executionTime-$numRuns.log			

			echo "collecting client4 log to result log"
			run_cmd $client4 "cat stdout.txt" > results-micro/client2$sampleSize-$maxTxSize-$workers-$ratio_i-$executionTime-$numRuns.log		

	

		done
	done
done



