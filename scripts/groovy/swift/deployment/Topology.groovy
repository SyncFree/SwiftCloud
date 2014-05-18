package swift.deployment

import static swift.deployment.Tools.*
import static swift.deployment.SwiftBase.*

class Topology {
		static List datacenters = []
		static List scoutGroups = []
		
		static String dcKey( int index ) {
			return "" + ('XYZWVUABCDEFGHIJKLM').charAt(index);
		}
		
		def static Datacenter DC(List sequencers, List surrogates) {
			return new Datacenter( sequencers, surrogates)
		}

		def static ScoutGroup SGroup(List clients, Datacenter dc) {
			return new ScoutGroup( clients, dc)
		}
		
		def static List allMachines() {
			def res = []
			datacenters.each {
				res += it.sequencers
				res += it.surrogates
			}
			res += scouts()
			res = res.unique()
		}
		
		def static List scouts() {
			def res = []
			scoutGroups.each{
				res += it.all()
			}
			return res
		}
		
		def static int totalScouts() {
			int res = 0;
			scoutGroups.each{
				res += it.all().size()
			}
			return res
		}

	static class Datacenter {
		def List sequencers
		def List surrogates
	
		def public Datacenter(List sequencers, List surrogates) {
			this.sequencers = sequencers.unique()
			this.surrogates = surrogates.unique()
			Topology.datacenters += this;
		}
				
		def List all() {
			return (sequencers + surrogates).unique()
		}	
		
		
		static def List sequencers() {
			def res = [] ;
			Topology.datacenters.each { dc ->
				res += dc.sequencers
			}
			return res.unique()
		}
		
		void deploySequencers(String shepard, String seqHeap = "256m") {
			def otherSequencers = sequencers() - this.sequencers
			def siteId = dcKey( Topology.datacenters.indexOf(this));
			
			sequencers.each { host ->		
				rshC(host, swift_app_cmd( "-Xms"+seqHeap, sequencerCmd(siteId, shepard, surrogates, otherSequencers), "seq-stdout.txt", "seq-stderr.txt" ))
			}
    	}	
    	
    	void deploySurrogates(String shepard, String surHeap = "512m") {
			def siteId = dcKey( Topology.datacenters.indexOf(this));
			
			surrogates.each { host ->
				def otherSurrogates = surrogates - host
            	rshC(host, swift_app_cmd( "-Xms"+surHeap, surrogateCmd( siteId, shepard, sequencers[0], otherSurrogates ), "sur-stdout.txt", "sur-stderr.txt" ))
			}			
    	}	
    	
	}
	
	
	static class ScoutGroup {
		List scouts
		Datacenter dc
		
		ScoutGroup( List scouts, Datacenter dc) {
			this.scouts = scouts.unique()
			this.dc = dc	
			Topology.scoutGroups += this
		} 
		
		void deploy( Closure cmd, Closure resHandler, String heap="512m") {
			Parallel.rsh( scouts, cmd, resHandler, true, 500000)
    	}
	
	
		def List all() {
			return scouts
		}	
	}
	
}
