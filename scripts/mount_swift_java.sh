#!/bin/bash

#This script launches the JVM directly which loads the fuse native library.
#It is not possible to fork a running JVM into the background, hence this script can only be used in the foreground.
#Which is probably the best option for debugging.
#If you want to fork into the background use the native launcher.

export PROJECT_NAME=fuse4j
CWD=`pwd`

export FUSE_HOME=/usr/local
export MOUNT_POINT=${CWD}/root
export FS_CLASS=FS_CLASS=swift/application/application/filesystem/cs/SwiftFuseClient

export VERSION=N2.4.0.0-SNAPSHOT
#JAVA_HOME=/usr/lib/java


export M2_REPO=${HOME}/.m2/repository

export LD_LIBRARY_PATH=/usr/local/lib:${JAVA_HOME}/jre/lib/i386/server:${CWD}/../native

java -Djava.library.path=/usr/local/lib swift.application.filesystem.cs.SwiftFuseClient localhost $MOUNT_POINT -f
