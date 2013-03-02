#! /bin/bash
# ec2-start-servers <server1 server2 ...>
. scripts/planetlab/pl-common.sh


#DEBUG_SEQ="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.port=6666 -Djava.rmi.server.hostname=$seq"

#DEBUG_SRV="swift_app_cmd -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.port=6666 -Djava.rmi.server.hostname=$seq"



servers_start() {
    local SERVERS=$1/Users/smd/Dropbox/bitbucket-git/swiftcloud-gforce/scripts/planetlab/pl-start-servers-ds-seq-dht.sh
    local SEQUENCERS=$2

    local sequencer_list="${SEQUENCERS[*]}"


    echo "Starting swift sequencers: " $other_seq

    exit

    i=0;
    for $seq in ${SEQUENCERS[*]} do

    #swift_app_cmd  $DEBUG_SEQ -Djava.rmi.server.hostname=$seq swift.dc.DCSequencerServer -name "X$i" -servers $srv -sequencers $other_seq

        local other_seq=${$sequencer_list[*]//$seq/}

        if [ -n other_seq ]
            then
                swift_app_cmd -Xmx1024m swift.dc.DCSequencerServer -name "X$i" -sequencers $other_seq
        elif
                swift_app_cmd -Xmx1024m swift.dc.DCSequencerServer -name "X$i"
        fi

        run_cmd_bg $seq $CMD
        i=$(($i+1))
    done
    sleep 2

    SEQ_NUMBER=$i

    echo "Starting swift servers: " $server_list
    
    i=0
    for srv in ${SERVERS[*]} do

        j=$(($i % $SEQ_NUMBER))
        seq=${SEQUENCERS[$j]}

        swift_app_cmd -Xmx512m swift.dc.DCServer -sequencer $seq
        run_cmd_bg $srv $CMD

        i=$(($i+1))
    done
}
