#!/bin/bash
# Execute from root SwiftCloud directory.

export EC2_IDENTITY_FILE=swiftcloud.pem
export EC2_USER=ubuntu
export EC2_EU_SERVER1=ec2-176-34-221-41.eu-west-1.compute.amazonaws.com
export EC2_EU_SERVER2=ec2-176-34-73-193.eu-west-1.compute.amazonaws.com
export EC2_US_SERVER=ec2-50-112-46-117.us-west-2.compute.amazonaws.com
export EC2_US_CLIENT=ec2-184-72-10-67.us-west-1.compute.amazonaws.com
export EC2_ALL="$EC2_EU_SERVER1 $EC2_EU_SERVER2 $EC2_US_SERVER $EC2_US_CLIENT"
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
	ssh -i $EC2_IDENTITY_FILE "$EC2_USER@$server" "$cmd"
}

# run_cmd_bg <server> <cmd>
run_cmd_bg() {
	server=$1
	shift
	cmd=$*
	echo "Running command on $server and detaching"
	echo "$cmd"
	ssh -fi $EC2_IDENTITY_FILE "$EC2_USER@$server" "$cmd"
}

# swift_app_cmd <class> <args>
# output in CMD
swift_app_cmd() {
	class=$1
	shift
	java_args=$*
	CMD=$(cat <<EOF
		if [ ! -f "$JAR" ]; then
			echo "$JAR not found" && exit 1;
		fi;
		java -cp $JAR -Djava.util.logging.config.file=$PROPS $class $java_args 2> >(tee stderr.txt 1>&2) > >(tee stdout.txt)
EOF
)
}

# kill_swift <server>
kill_swift() {
	server=$1
	run_cmd $server "killall java && sleep 1 && killall -9 java"
}

run_swift_app_detach() {
	server=$1
	class=$2
	args=$3
	run_swift_app $1 $2 $3
}

# copy_to <local_file> <server> <remote_file>
copy_to() {
	echo "Copying to $2..."
	rsync -e "ssh -i $EC2_IDENTITY_FILE" "$1" "$EC2_USER@$2:$3"
}

# copy_from <server> <remote file> <local file>
copy_from() {
	echo "Copying from $1..."
	scp -i $EC2_IDENTITY_FILE "$EC2_USER@$1:$2" "$3"
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
	primary=$1
	echo "Installing on primary: $primary"
	deploy_swift_on $primary
	shift
	for server in $*; do
		for f in $SWIFT_FILES; do
			run_cmd $primary rsync -e ssh $f $server:$f
		done
	done
}

