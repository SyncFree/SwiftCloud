#! /bin/bash
# ec2-start-servers <server1 server2 ...>
. scripts/planetlab/pl-common.sh

servers_start() {
    local arg1="$1[*]"
    local arg2="$2[*]"

    local server_list=${!arg1}
    local sequencer_list=${!arg2}

    local sequencer_array=($sequencer_list)

    echo
    echo "***** Starting swift sequencers: " $sequencer_list

    i=0
    for srv in $sequencer_list; do
        seq=${sequencer_array[$i]}
        other_seq=${sequencer_list//$seq/}

        swift_app_cmd -Xmx512m swift.dc.DCSequencerServer -name "X$i" -servers $srv -sequencers $other_seq

        run_cmd_bg $seq $CMD

        i=$(($i+1))
    done

    SEQ_NUMBER=${#sequencer_array[@]}

    echo "***** Starting swift servers: " $server_list

    i=0
    for srv in $server_list; do

        j=$(($i % $SEQ_NUMBER))
        seq=${sequencer_list[$j]}


		swift_app_cmd -Xmx512m swift.dc.DCServer -sequencer $seq

        run_cmd_bg $srv $CMD

        i=$(($i+1))
        sleep 5
	done
}