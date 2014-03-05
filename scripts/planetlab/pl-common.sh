#!/bin/bash
# Execute from root SwiftCloud directory.

export EC2_IDENTITY_FILE=swiftcloud.pem
export EC2_USER=fctple_SwiftCloud

if [ -z $DUMMY ]; then
	SSH=ssh
	SCP=scp
	RSYNC=rsync
else
	SSH=echo
	SCP=echo
	RSYNC=echo
fi

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
	$SSH "$EC2_USER@$server" "$cmd"
}

# run_cmd_bg <server> <cmd>
run_cmd_bg() {
	server=$1
	shift
	cmd=$*
	echo "Running command on $server and detaching"
	echo "$cmd"
	$SSH "$EC2_USER@$server" "$cmd" &
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
