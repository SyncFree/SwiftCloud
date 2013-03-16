#! /bin/bash

export DATACENTER_SERVERS=(
    ec2-54-228-106-66.eu-west-1.compute.amazonaws.com
    ec2-54-249-137-48.ap-northeast-1.compute.amazonaws.com
    ec2-50-112-200-169.us-west-2.compute.amazonaws.com
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

