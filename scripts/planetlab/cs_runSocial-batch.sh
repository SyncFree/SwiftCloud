#!/bin/bash

# INPUT DATA PARAMS
export DURATION=60
export THINK_TIME_MS=0
export CACHE_EVICTION_TIME_MS=120000 #120000

export INPUT_USERS=1500
export INPUT_ACTIVE_USERS=$(($CLIENTS_NUMBER*1))

export INPUT_USER_FRIENDS=25
export INPUT_USER_BIASED_OPS=9
export INPUT_USER_RANDOM_OPS=1
export INPUT_USER_OPS_GROUPS=500
export MAX_CONCURRENT_SESSIONS_PER_JVM=$INPUT_ACTIVE_USERS


# BENCHMARK PARAMS
export CACHING=CACHED
export ASYNC_COMMIT=false
export NOTIFICATIONS=false
export ISOLATION=REPEATABLE_READS

. ./scripts/planetlab/cs_runSocial-cdn-4batch.sh
. ./scripts/planetlab/cs_runSocial-cdn-4batch.sh

. ./scripts/planetlab/cs_runSocial-osdi-4batch.sh
. ./scripts/planetlab/cs_runSocial-osdi-4batch.sh

. ./scripts/planetlab/cs_runSocial-dc-4batch.sh
. ./scripts/planetlab/cs_runSocial-dc-4batch.sh

export CACHING=CACHED
export ASYNC_COMMIT=false
export NOTIFICATIONS=false
export ISOLATION=SNAPSHOT_ISOLATION

. ./scripts/planetlab/cs_runSwiftDoc-cdn-4batch.sh
. ./scripts/planetlab/cs_runSwiftDoc-cdn-4batch.sh

. ./scripts/planetlab/cs_runSwiftDoc-osdi-4batch.sh
. ./scripts/planetlab/cs_runSwiftDoc-osdi-4batch.sh

. ./scripts/planetlab/cs_runSwiftDoc-dc-4batch.sh
. ./scripts/planetlab/cs_runSwiftDoc-dc-4batch.sh



