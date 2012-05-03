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
export EC2_EU_SERVER1=ec2-176-34-221-41.eu-west-1.compute.amazonaws.com
export EC2_EU_SERVER2=ec2-176-34-73-193.eu-west-1.compute.amazonaws.com
export EC2_US_SERVER=ec2-50-112-46-117.us-west-2.compute.amazonaws.com
export EC2_US_CLIENT=ec2-184-72-10-67.us-west-1.compute.amazonaws.com

export EC2_US_OREGON=ec2-50-112-199-43.us-west-2.compute.amazonaws.com
export EC2_US_NORTHCALIFORNIA1=ec2-184-169-233-51.us-west-1.compute.amazonaws.com
export EC2_US_NORTHCALIFORNIA2=ec2-50-18-133-148.us-west-1.compute.amazonaws.com
export EC2_EU1=ec2-79-125-37-63.eu-west-1.compute.amazonaws.com
export EC2_EU2=ec2-46-51-165-88.eu-west-1.compute.amazonaws.com
export EC2_EU3=ec2-176-34-173-115.eu-west-1.compute.amazonaws.com
export EC2_ASIA_SINGAPORE1=ec2-122-248-226-204.ap-southeast-1.compute.amazonaws.com
export EC2_ASIA_SINGAPORE2=ec2-46-137-229-245.ap-southeast-1.compute.amazonaws.com
export EC2_ASIA_TOKYO=ec2-54-248-17-129.ap-northeast-1.compute.amazonaws.com


# CAREFUL: Depending on the settings EC2_ALL needs to be adapted
export EC2_ALL="$EC2_EU1 $EC2_EU2 $EC2_EU3 $EC2_US_OREGON $EC2_US_NORTHCALIFORNIA1 $EC2_US_NORTHCALIFORNIA2 $EC2_ASIA_TOKYO $EC2_ASIA_SINGAPORE1 $EC2_ASIA_SINGAPORE2"
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

deploy_swift_on_many() {
	echo "Installing swift on: $*"
#	primary=$1
#	echo "Installing on primary: $primary"
#	deploy_swift_on $primary
#	shift
#	rsync_cmd=""
	for server in $*; do
		deploy_swift_on $server
#		rsync_cmd="$rsync_cmd; $primary rsync -e ssh $SWIFT_FILES $server:"
	done
#	echo "Copying from primary to other servers."
#	run_cmd $rsync_cmd
}

