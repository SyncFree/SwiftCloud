#!/bin/bash
# Execute from root SwiftCloud directory.

if [ -z $DUMMY ]; then
	SSH=ssh
	SCP=scp
	RSYNC=rsync
else
	SSH=echo
	SCP=echo
	RSYNC=echo
fi

export EC2_IDENTITY_FILE=swiftcloud.pem
export EC2_USER=fctple_SwiftCloud

# PRODUCTION instances
export EC2_PROD_EU_C1MEDIUM=(
)


export EC2_PROD_EU_MICRO=(
planetlab-um10.di.uminho.pt
planetlab1.fct.ualg.pt
)

export PLANETLAB_SCOUTS=(
planetlab2.fct.ualg.pt
planetlab-um00.di.uminho.pt
)

export PLANETLAB_CLIENTS=(
planetlab1.di.fct.unl.pt
planetlab2.di.fct.unl.pt
)

export PLANETLAB_NODES=(
planetlab-1.iscte.pt
planetlab-2.iscte.pt
)


# WARNING - PlanetLab nodes are volatile; some may be down...
export PLANETLAB_NODES_ALL=(
ait21.us.es
ait05.us.es
planetlab2.di.fct.unl.pt
planetlab-1.iscte.pt

ple2.ipv6.lip6.fr
peeramide.irisa.fr
planetlab-2.imag.fr
planetlab1.fct.ualg.pt
planetlab2.fct.ualg.pt
planetlab-um10.di.uminho.pt
planetlab-um00.di.uminho.pt
planetlab1.fct.ualg.pt
planetlab1.di.fct.unl.pt
planetlab2.di.fct.unl.pt
planetlab-1.tagus.ist.utl.pt
planetlab-2.tagus.ist.utl.pt
planetlab-1.tagus.ist.utl.pt
planetlab-2.tagus.ist.utl.pt
planetlab1.eurecom.fr
planetlab2.eurecom.fr

)

# TEST instances
export EC2_TEST_EU=(
)

# CAREFUL: Depending on the settings EC2_ALL needs to be adapted
export EC2_ALL="${EC2_TEST_EU[*]}"
export JAR=swiftcloud.jar
export PROPS=all_logging.properties
export SWIFT_FILES="$JAR $PROPS"

# run_cmd <server> <cmd>
run_cmd() {
	server=$1
	shift
	cmd=$*
	echo "Running command on $server"
	echo "$cmd"
	$SSH -i $EC2_IDENTITY_FILE "$EC2_USER@$server" "$cmd"
}

# run_cmd_bg <server> <cmd>
run_cmd_bg() {
	server=$1
	shift
	cmd=$*
	echo "Running command on $server and detaching"
	echo "$cmd"
	$SSH -i $EC2_IDENTITY_FILE "$EC2_USER@$server" "$cmd" &
}

# swift_app_cmd <java options>
# output in CMD
swift_app_cmd() {
	swift_app_cmd_raw "$* 2> >(tee stderr.txt 1>&2) > >(tee stdout.txt)"
}

swift_app_cmd_nostdout() {
	swift_app_cmd_raw "$* 2> >(tee stderr.txt 1>&2) > stdout.txt"
}

swift_app_cmd_raw() {
	args=$*
	CMD=$(cat <<EOF
		if [ ! -f "$JAR" ]; then
			echo "$JAR not found" && exit 1;
		fi;
		java -cp $JAR -Djava.util.logging.config.file=$PROPS $args
EOF
)
}

# kill_swift <server>
kill_swift() {
	server=$1
	run_cmd $server "killall java && sleep 1 && killall -9 java"
}

# copy_to <local_file> <server> <remote_file>
copy_to() {
	echo "Copying to $2..."
    echo "ssh -i $EC2_IDENTITY_FILE" "$1" "$EC2_USER@$2:$3"
	$RSYNC -e "ssh -i $EC2_IDENTITY_FILE" "$1" "$EC2_USER@$2:$3"
}

# copy_to_bg <local_file> <server> <remote_file>
copy_to_bg() {
	echo "Copying to $2 in background..."
#	$RSYNC -e "ssh -i $EC2_IDENTITY_FILE" "$1" "$EC2_USER@$2:$3" &
	$RSYNC -e "ssh -i $EC2_IDENTITY_FILE" "$1" "$EC2_USER@$2:$3"
}

# copy_from <server> <remote file> <local file>
copy_from() {
	echo "Copying from $1..."
	$SCP -i $EC2_IDENTITY_FILE "$EC2_USER@$1:$2" "$3"
}

# deploy_swift_to <server>
deploy_swift_on() {
	echo "Installing swift on: $1"
	server=$1
	copy_to $JAR $server $JAR
	copy_to stuff/$PROPS $server $PROPS	
}

# deploy_swift_to_many <server1, server2, server3... >
deploy_swift_on_many2() {
	echo "Installing swift on: $*"
	for server in $*; do
        deploy_swift_on $server
	done
}

# deploy_swift_to_many <server1, server2, server3... >
deploy_swift_on_many() {
	echo "Installing swift on: $*"
	primary=$1
	echo "Installing on the primary first"
	deploy_swift_on $primary
	shift
	rsync_cmd=":" # no-op
	rsync_ec2_int="$RSYNC -e '$SSH -o StrictHostKeyChecking=no'"
	for server in $*; do
		# deploy_swift_on $server
		rsync_cmd="$rsync_cmd; $rsync_ec2_int $SWIFT_FILES $server:"
	done
	echo "Copying from the primary to other servers."
	run_cmd $primary $rsync_cmd
}