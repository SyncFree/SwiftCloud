#!/bin/bash
# ec2-run <server> <class> <args>
. scripts/ec2-common.sh

machines=$*
for i in $machines; do
	# do not repide two side!!
	for j in $machines; do
		PING="ping -c 5 $j"
		run_cmd "$i" "$PING" > results/pingtime_"$i"_"$j".log
	done
done

