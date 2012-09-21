#! /bin/bash
# ec2-start-servers <server1 server2 ...>
. scripts/planetlab/pl-common.sh

servers_start() {
    local arg1="$1[*]"
    local arg2="$2[*]"

    local server_list=${!arg1}
    local sequencer_list=${!arg2}

    local sequencer_array=($sequencer_list)

	echo "Starting swift servers: " $server_list
    echo "Starting swift sequencers: " $sequencer_list
	
    i=0
    for srv in $server_list; do
        seq=${sequencer_array[$i]}
        other_seq=${sequencer_list//$seq/}

#        swift_app_cmd -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.port=9418 -Djava.rmi.server.hostname=$seq swift.dc.DCSequencerServer -name "X$i" -servers $srv -sequencers $other_seq

        #swift_app_cmd swift.dc.DCSequencerServer -name "X$i"
        swift_app_cmd swift.dc.DCSequencerServer -name "X$i" -servers $srv -sequencers $other_seq

        run_cmd_bg $seq $CMD

		sleep 2
		swift_app_cmd -Xmx512m swift.dc.DCServer -sequencer $seq
        run_cmd_bg $srv $CMD

        i=$(($i+1))
	done
}
# 
