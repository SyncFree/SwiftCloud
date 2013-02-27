#! /bin/bash

controlC() {
echo "Ctl+C Detected... Exiting!"
echo "==== KILLING EXISTING SERVERS AND CLIENTS ===="
parallel-nuke -l fctple_SwiftCloud -h /tmp/nodes.txt java
echo "==== DONE ===="
exit 1
}

trap controlC SIGINT SIGTERM

. ./scripts/planetlab/pl-common.sh


export DATACENTER_SERVERS=(
peeramide.irisa.fr
)


export SCOUT_NODES=(
planetlab1.di.fct.unl.pt
planetlab2.di.fct.unl.pt
planetlab-1.iscte.pt
planetlab-3.iscte.pt
planetlab-4.iscte.pt
)


# TOPOLOGY
DCS=("${DATACENTER_SERVERS[@]}")
DCSEQ=("${DATACENTER_SERVERS[@]}")

SCOUTS=("${SCOUT_NODES[@]}")
ENDCLIENTS=("${SCOUT_NODES[@]}")

MACHINES="${DCS[*]} ${ENDCLIENTS[*]}"

rm -f /tmp/nodes.txt
rm -f /tmp/nodes2.txt
for i in $MACHINES; do
echo $i >> /tmp/nodes.txt
#host $i | grep address | awk '{ print $4 }' >> /tmp/nodes2.txt
done
cat /tmp/nodes.txt

rm -f /tmp/scouts.txt
for scout in ${SCOUTS[*]}; do
echo $scout >> /tmp/scouts.txt
done

INIT_DB_DC=${DCS[0]}
INIT_DB_CLIENT=${DCS[0]}
WEBSERVER=${DCS[0]}

DC_NUMBER=${#DCS[@]}
SCOUTS_NUMBER=${#SCOUTS[@]}
CLIENTS_NUMBER=${#ENDCLIENTS[@]}

DURATION=300
SESSIONS_PER_SCOUT=3

SHEPARD=$INIT_DB_DC
CONFIG=swiftsocial-test
CONFIG_FILE=$CONFIG.props

SCOUT_JAVACMD="java -Xincgc -Xms512m -Xmx512m -Djava.util.logging.config.file=all_logging.properties -Dswiftsocial=$CONFIG_FILE -cp swiftcloud.jar swift.application.social.SwiftSocialBenchmark"

SCOUT_CMD="$SCOUT_JAVACMD run $SHEPARD $SESSIONS_PER_SCOUT scouts.txt -servers "${DATACENTER_SERVERS[*]}" > stdout.txt 2> stderr.txt < /dev/null &"

echo "DCS: " $DC_NUMBER "CLIENTS: " $CLIENTS_NUMBER


echo "==== KILLING EXISTING SERVERS AND CLIENTS ===="
parallel-nuke -v -l fctple_SwiftCloud -h /tmp/nodes.txt java
echo "==== DONE ===="

# run_swift_client_initdb <client> <server>
run_swift_client_initdb() {
	client=$1
	server=$2
	config_file=$3.props
	swift_app_cmd -Dswiftsocial=$config_file swift.application.social.SwiftSocialBenchmark init $server
	run_cmd $client $CMD
}

ant -buildfile smd-jar-build.xml

if [ ! -f "$JAR" ]; then
echo "file $JAR not found" && exit 1
fi

echo $CONFIG

echo "Deploying swiftcloud.jar"
#rsync --progress -u swiftcloud.jar fctple_SwiftCloud@$WEBSERVER:
#pssh -t 300 -p 64 -l fctple_SwiftCloud -h /tmp/nodes.txt wget -N $WEBSERVER/swiftcloud.jar
prsync -v -p 10 -l fctple_SwiftCloud -h /tmp/nodes.txt swiftcloud.jar /home/fctple_SwiftCloud/swiftcloud.jar

echo "Deploying $CONFIG.props"
#rsync -u $CONFIG.props fctple_SwiftCloud@$WEBSERVER:
#pssh -t 180 -p 64 -l fctple_SwiftCloud -h /tmp/nodes.txt wget -N $WEBSERVER/$CONFIG.props
prsync -p 100 -l fctple_SwiftCloud -h /tmp/nodes.txt $CONFIG.props /home/fctple_SwiftCloud/$CONFIG.props

echo "Deploying scouts.txt"
#rsync -u /tmp/scouts.txt fctple_SwiftCloud@$WEBSERVER:
#pssh -t 180 -p 64 -l fctple_SwiftCloud -h /tmp/nodes.txt wget -N $WEBSERVER/scouts.txt
prsync -p 100 -l fctple_SwiftCloud -h /tmp/scouts.txt /tmp/scouts.txt /home/fctple_SwiftCloud/scouts.txt

#echo "Deploying all_logging.properties"
#rsync -u stuff/all_logging.properties fctple_SwiftCloud@$WEBSERVER:all_logging.properties
#pssh -t 180 -p 64 -l fctple_SwiftCloud -h /tmp/nodes.txt wget -N $WEBSERVER/all_logging.properties
prsync -p 100 -l fctple_SwiftCloud -h /tmp/nodes.txt stuff/all_logging.properties /home/fctple_SwiftCloud/all_logging.properties

#ssh fctple_SwiftCloud@$INIT_DB_DC "scp -v -p 64 -t 300 -l fctple_SwiftCloud -h scouts.txt scouts.txt /home/fctple_SwiftCloud/scouts.txt"
#ssh fctple_SwiftCloud@$INIT_DB_DC "prsync -v -p 64 -t 300 -l fctple_SwiftCloud -h scouts.txt swiftcloud.jar /home/fctple_SwiftCloud/swiftcloud.jar"
#ssh fctple_SwiftCloud@$INIT_DB_DC "prsync -v -p 64 -t 300 -l fctple_SwiftCloud -h scouts.txt $CONFIG.props /home/fctple_SwiftCloud/$CONFIG.props"
#ssh fctple_SwiftCloud@$INIT_DB_DC "prsync -v -p 64 -t 300 -l fctple_SwiftCloud -h scouts.txt all_logging.properties /home/fctple_SwiftCloud/all_logging.properties"


echo "==== STARTING SWIFT SHEPARD, DURATION: " $DURATION
swift_app_cmd_nostdout sys.shepard.Shepard -duration $DURATION
run_cmd_bg $SHEPARD $CMD

. scripts/planetlab/pl-start-servers-ds-seq.sh
echo "==== STARTING SEQUENCERS" $DCSEQ "AND DC SERVERS ====" $DCS
servers_start DCS DCSEQ

echo "==== WAITING A BIT BEFORE INITIALIZING DATABASE ===="
sleep 10

echo "==== INITIALIZING DATABASE ===="
echo "CLIENT" + $INIT_DB_CLIENT, "SERVER" + $INIT_DB_DC
run_swift_client_initdb $INIT_DB_CLIENT $INIT_DB_DC $CONFIG

echo "==== WAITING A BIT BEFORE STARTING REAL CLIENTS ===="
sleep 10

echo "DCS: " $DC_NUMBER "CLIENTS: " $CLIENTS_NUMBER

echo "==== STARTING SCOUT+SWIFTSOCIAL SCOUTS"
pssh -v -t 120 -p 100 -l fctple_SwiftCloud -h /tmp/scouts.txt nohup $SCOUT_CMD
echo $SCOUT_CMD


echo "==== RUNNING... ===="

sleep $DURATION
sleep 60

echo "==== KILLING SERVERS AND CLIENTS ===="
parallel-nuke -p 100 -l fctple_SwiftCloud -h /tmp/nodes.txt java

echo "==== COLLECTING CLIENT LOGS AS RESULTS ===="

runDir="results/swiftsocial/"`date "+%b%s"`
resName=1pc-results-swiftsocial-DC-$DC_NUMBER-SC-$SCOUTS_NUMBER-TH-$SESSIONS_PER_SCOUT.log
echo $runDir
mkdir -p $runDir
output_prefix=$runDir/$resName
mkdir -p $output_prefix
pslurp -l fctple_SwiftCloud -h /tmp/scouts.txt -L $output_prefix /home/fctple_SwiftCloud/stdout.txt $resName

ls -lR $output_prefix | grep .log

