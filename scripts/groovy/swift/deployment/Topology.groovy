package swift.deployment

import java.lang.management.ManagementFactory;
import java.util.Random
import static swift.deployment.Tools.*
import static swift.deployment.SwiftBase.*

class Topology {
    static List datacenters = []
    static List scoutGroups = []

    static String dcKey( int index ) {
        return "" + ('XYZABCDEFGHIJKLMNOPQRSTUVWxyzabcdefghijklmnopqrstuvw').charAt(index);
    }

    static void clear() {
        datacenters.clear()
        scoutGroups.clear()
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
            def res = [];
            Topology.datacenters.each { dc ->
                res += dc.sequencers
            }
            return res.unique()
        }

        void deploySequencersExtraArgs(String shepard, String extraArgs, String seqHeap = "256m") {
            def otherSequencers = sequencers() - this.sequencers
            def siteId = dcKey( Topology.datacenters.indexOf(this));

            sequencers.each { host ->
                rshC(host, swift_app_cmd_nostdout( "-Xms"+seqHeap, sequencerCmd(siteId, shepard, surrogates, otherSequencers, extraArgs), "seq-stderr.txt", "seq-stdout.txt" ))
            }
        }

        void deploySequencers(String shepard, String seqHeap = "256m") {
            deploySequencersExtraArgs( shepard, "", seqHeap)
        }

        void deploySurrogatesExtraArgs(String shepard, String extraArgs, String surHeap = "512m") {
            def siteId = dcKey( Topology.datacenters.indexOf(this));

            surrogates.each { host ->
                def otherSurrogates = surrogates - host
                rshC(host, swift_app_cmd_nostdout( "-Xms"+surHeap, surrogateCmd( siteId, shepard, sequencers[0], otherSurrogates, extraArgs ), "sur-stderr.txt", "sur-stdout.txt" ))
            }
        }

        void deployIntegratedSurrogatesExtraArgs(String shepard, String extraArgs, String surHeap = "512m") {
            def siteId = dcKey( Topology.datacenters.indexOf(this));

            def otherSequencers = sequencers() - this.sequencers
            def seqArgs = "-integrated -sequencers "
            otherSequencers.each { seqArgs += it + " "}

            surrogates.each { host ->
                def otherSurrogates = surrogates - host
                rshC(host, swift_app_cmd_nostdout( "-Xms"+surHeap, surrogateCmd( siteId, shepard, sequencers[0], otherSurrogates, seqArgs + extraArgs ), "sur-stderr.txt", "sur-stdout.txt" ))
            }
        }
        void deploySurrogates(String shepard, String surHeap = "512m") {
            deploySurrogatesExtraArgs( shepard, "", surHeap)
        }
    }


    static class ScoutGroup {
        List scouts
        Datacenter dc

        ScoutGroup( List scouts, Datacenter dc) {
            this.scouts = scouts
            this.dc = dc
            Topology.scoutGroups += this
        }

        void deploy( Closure cmd, Closure resHandler) {
            Parallel.rsh( scouts, cmd, resHandler, false, 500000)
        }


        def List all() {
            return scouts
        }
    }

    static int ACQUIRE_WAIT_MAX_MS = 2000

    static File acquireTopologyFile(String fileNamePrefix) {
        def random = new Random();
        File acquiredConfig = null
        File acquiredConfigRenamed = null
        def pid = ManagementFactory.getRuntimeMXBean().getName()
        while (acquiredConfig == null) {
            // Everyone needs to wait in the first place to achieve fairness
            sleep(random.nextInt(ACQUIRE_WAIT_MAX_MS))
            for (File child : new File("scripts/groovy/swift/deployment/").listFiles()) {
                if (child.name.startsWith(fileNamePrefix) && child.name.endsWith(".groovy")) {
                    File candidateConfigRenamed = new File(child.absolutePath + ".locked." + pid)
                    if (child.renameTo(candidateConfigRenamed)) {
                        acquiredConfig = child
                        acquiredConfigRenamed = candidateConfigRenamed
                        break
                    }
                }
            }
            if (acquiredConfig == null) {
                println("No topology configuration available - retrying in max. " + ACQUIRE_WAIT_MAX_MS + "ms")
            }
        }

        println "Topology configuration " + acquiredConfig + " acquired by process " + pid
        addShutdownHook {
            if (acquiredConfigRenamed.renameTo(acquiredConfig)) {
                println "Topology configuration " + acquiredConfig + " released"
            } else {
                println "WARNING: could not release topology configuration " + acquiredConfig + " acquired by process" + pid
            }
        }
        return acquiredConfigRenamed
    }
}
