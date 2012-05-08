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
export EC2_USER=ubuntu

# TEST instances
export EC2_TEST_EU=(
ec2-176-34-82-18.eu-west-1.compute.amazonaws.com 
ec2-46-137-37-255.eu-west-1.compute.amazonaws.com
ec2-46-137-139-255.eu-west-1.compute.amazonaws.com
ec2-176-34-222-247.eu-west-1.compute.amazonaws.com)

# CAREFUL: Depending on the settings EC2_ALL needs to be adapted
export EC2_ALL="${EC2_TEST_EU[*]}"
export JAR=swiftcloud.jar
export PROPS=deployment_logging.properties
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
	$SSH -fi $EC2_IDENTITY_FILE "$EC2_USER@$server" "$cmd"
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
	$RSYNC -e "ssh -i $EC2_IDENTITY_FILE" "$1" "$EC2_USER@$2:$3"
}

# copy_to_bg <local_file> <server> <remote_file>
copy_to_bg() {
	echo "Copying to $2 in background..."
	$RSYNC -e "ssh -i $EC2_IDENTITY_FILE" "$1" "$EC2_USER@$2:$3" &
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

