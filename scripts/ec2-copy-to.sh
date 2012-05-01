#!/bin/bash
. scripts/ec2-common.sh

if [ -z "$*" ]; then
        echo "ec2-copy-to.sh <local filename> <ec2-instance> <remote filename>"
        echo "Copies given local file to provided ec2 host." 
	exit 1
fi

lfile=$1
server=$2
rfile=$3
copy_to $lfile $server $rfile

