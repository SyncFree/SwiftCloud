#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;


def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})


EuropeEC2 = [
    'ec2-54-76-240-201.eu-west-1.compute.amazonaws.com',
    'ec2-54-76-10-200.eu-west-1.compute.amazonaws.com',
    'ec2-54-76-240-137.eu-west-1.compute.amazonaws.com',
//    'ec2-54-76-228-192.eu-west-1.compute.amazonaws.com',
//    'ec2-54-76-160-244.eu-west-1.compute.amazonaws.com',
//    'ec2-54-76-182-150.eu-west-1.compute.amazonaws.com',
//    'ec2-54-76-157-46.eu-west-1.compute.amazonaws.com'
]

Server = EuropeEC2[0]
Clients = EuropeEC2[1..EuropeEC2.size() - 1]

AllMachines = EuropeEC2

println getBinding().getVariables()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)

//System.exit(0)

println "==== BUILDING JAR "
sh("ant -buildfile smd-jar-build.xml").waitFor()
deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, "stuff/logging.properties", "logging.properties")


println "==== LAUNCHING RPC SERVER"
rshC(Server, SwiftBase.swift_app_cmd_nostdout("", "-cp swiftcloud.jar sys.net.impl.RpcClientServer ", "sur-stdout.txt", "sur-stderr.txt" ))

Sleep(10)
println "==== LAUNCHING RPC CLIENTS"
Clients.each { client ->
    rshC(client, SwiftBase.swift_app_cmd_nostdout("", "-cp swiftcloud.jar sys.net.impl.RpcClientServer " + Server +" ", "sur-stdout.txt", "sur-stderr.txt" ))
}

Countdown( "Max. remaining time: ", 1000)

System.exit(0)

