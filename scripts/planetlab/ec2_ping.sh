#! /bin/bash

export DATACENTER_SERVERS=(
    ec2-54-228-60-16.eu-west-1.compute.amazonaws.com
    ec2-50-112-87-147.us-west-2.compute.amazonaws.com
    ec2-54-241-199-234.us-west-1.compute.amazonaws.com
    ec2-54-234-176-198.compute-1.amazonaws.com
)

DCS=("${DATACENTER_SERVERS[@]}")
MACHINES="${DCS[*]}"


rm -f .ec2-rtts
i=0;
for m in ${MACHINES[*]}; do
    echo $m
    echo $m >> .ec2-rtts
    ping -a -q -c 20 $m >> .ec2-rtts
done

