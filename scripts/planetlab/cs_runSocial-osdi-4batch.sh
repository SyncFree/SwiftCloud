#! /bin/bash

echo "PLEASE CONFIGURE NODES"
exit

. ./scripts/planetlab/pl-common.sh

SCENARIO="osdi"

export DATACENTER_SERVERS=(
ec2-176-34-78-57.eu-west-1.compute.amazonaws.com
)

export SCOUT_NODES=(
ait21.us.es
ait05.us.es
)


export ENDCLIENT_NODES=(
planetlab-3.iscte.pt
planetlab-4.iscte.pt
planetlab-1.tagus.ist.utl.pt
planetlab-2.tagus.ist.utl.pt
planetlab1.fct.ualg.pt
planetlab2.fct.ualg.pt
planetlab1.di.fct.unl.pt
planetlab2.di.fct.unl.pt
planetlab-um00.di.uminho.pt
planetlab-um10.di.uminho.pt
)

. ./scripts/planetlab/cs_runSocial-common-4batch.sh