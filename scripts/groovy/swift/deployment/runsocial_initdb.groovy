#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib

package swift.deployment

import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;


def EC_SERVER ="ec2-54-76-46-41.eu-west-1.compute.amazonaws.com"

def Servers = DC([EC_SERVER], [EC_SERVER]);

def Clients = SGroup( [ EC_SERVER], Servers)

def Scouts = ( Topology.scouts() ).unique()

def ShepardAddr = "localhost"

def AllMachines = ( Topology.allMachines() ).unique()

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})

println "==== BUILDING JAR..."

sh("ant -buildfile smd-jar-build.xml").waitFor()

deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, "stuff/logging.properties", "logging.properties")


30.times {
			def int numUsers = 3000 * (it+7)
			
			props = [
					'swiftsocial.numUsers':''+numUsers
			    ]
			
			def SwiftSocial_Props = 'swiftsocial-' + numUsers/1000+ 'k.props'
			
			    
			deployTo(AllMachines, SwiftSocial.genPropsFile(props, SwiftSocial.DEFAULT_PROPS).absolutePath, SwiftSocial_Props)
			 
			pnuke(AllMachines, "java", 60)
			    			
			println "==== INITIALIZING DATABASE ===="
			
			int threads = 16
			SwiftSocial2.prepareDB( EC_SERVER, EC_SERVER, SwiftSocial_Props, threads, "3000m")
			
			println "==== WAITING A BIT TO ALLOW DB TO BE FLUSHED TO DISK ===="
			Sleep(30)
			
			rsh(EC_SERVER, 'mv db/default db/' + numUsers/1000 + 'k')
			rsh(EC_SERVER, 'mv db/default_seq db/' + numUsers/1000 + 'k_seq')
	}