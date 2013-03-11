#! /bin/bash

export DATACENTER_SERVERS=(
    ec2-54-228-99-154.eu-west-1.compute.amazonaws.com
    ec2-50-18-226-88.us-west-1.compute.amazonaws.com
    ec2-184-72-158-91.compute-1.amazonaws.com
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

