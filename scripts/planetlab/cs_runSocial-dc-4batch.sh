#! /bin/bash

echo "PLEASE CONFIGURE NODES"
exit

. ./scripts/planetlab/pl-common.sh

SCENARIO="dc"


export DATACENTER_SERVERS=(
peeramide.irisa.fr
)

export SCOUT_NODES=(
peeramide.irisa.fr
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