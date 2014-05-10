package swift.deployment

import static swift.deployment.Tools.*

class SwiftBase {
    static String SURROGATE_CMD = "-Xincgc -cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties swift.dc.DCServer"
    static String SEQUENCER_CMD = "-Xincgc -cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties swift.dc.DCSequencerServer"
    static String SHEPARD_CMD = "-cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties sys.shepard.Shepard"

    static String swift_app_cmd( String heap, String exec, String stderr, String stdout ) {
        return "java " + heap + " " + exec + "2> >(tee " + stderr + " 1>&2) > >(tee " + stdout + ")"
    }

    static String swift_app_cmd_nostdout( String heap, String exec, String stdout, String stderr )  {
        return "java " + heap + " " + exec +  "2> >(tee " + stderr+ " 1>&2) > " + stdout
    }

    static String sequencerCmd( String name, List servers, List otherSequencers) {
        def res  = SEQUENCER_CMD + " -name " + name + " -servers "
        servers.each { res += it + " "}
        res += "-sequencers "
        otherSequencers.each { res += it + " "}
        return res
    }

    static String surrogateCmd( String sequencer ) {
        def res  = SURROGATE_CMD + " -sequencer " + sequencer + " "
        return res
    }

	static void runEachAsSequencer( List sequencers, List surrogates, String seqHeap) {
        println "==== STARTING DATACENTER SEQUENCERS ===="

        sequencers.each { host ->
            def sequencer = host
            def other_sequencers = sequencers.clone() - host
            def name = "X" + sequencers.indexOf(host)
            rshC(sequencer, swift_app_cmd( "-Xms"+seqHeap, sequencerCmd(name, surrogates, other_sequencers), "seq-stdout.txt", "seq-stderr.txt" ))
        }
        println "\nOK"
    }
 
 	static void runEachAsSurrogate( List surrogates, String sequencer, String heap) {
        println "==== STARTING DATACENTER SURROGATES ===="

        surrogates.each { host ->
            rshC(host, swift_app_cmd_nostdout( "-Xms"+heap, surrogateCmd( sequencer ), "sur-stdout.txt", "sur-stderr.txt" ))
            if( surrogates.indexOf( host ) == 0 )
            	Sleep(10);
        }
        println "\nOK"
    }
       
    static void runEachAsDatacentre( List datacentres, String seqHeap, String surHeap ) {
        println "==== STARTING DATACENTER SERVERS ===="

        int i = 0;
        datacentres.each {
            def srv = it
            def surrogate = srv
            def sequencer = srv
            def other_sequencers = datacentres.clone() - srv
            def name = "X" + i
            rshC(sequencer, swift_app_cmd( "-Xms"+seqHeap, sequencerCmd(name, [srv], other_sequencers), "seq-stdout.txt", "seq-stderr.txt" ))
            rshC(surrogate, swift_app_cmd( "-Xms"+surHeap, surrogateCmd( sequencer ), "sur-stdout.txt", "sur-stderr.txt" ))
            i++;
        }
        println "\nOK"
    }


    static def runShepard( host, duration, pattern ) {
        def queue = new java.util.concurrent.SynchronousQueue<?>()
        println "==== STARTING SHEPARD @ " + host + " DURATION: " + duration

        def cmd = SHEPARD_CMD + " -duration " + (int)duration + " "
        Process proc = rsh( host, swift_app_cmd("-Xmx64m", cmd, "shep-stdout.txt", "shep-stderr.txt") );
        Thread.start {
            proc.errorStream.withReader {
                String line;
                while ( (line = it.readLine() ) != null ) {
                    println line
                    if( line.contains( pattern ) ) {
                        queue.offer( proc )
                        return true
                    }
                }
            }
        }
        println "\nOK"
        return queue
    }

    static File genPropsFile(Map props, Map defaultProps = []) {
        File f = File.createTempFile("swif-", ".props")
        PrintWriter pw = f.newPrintWriter()
        (defaultProps.keySet() + props.keySet()).unique().each { k ->
            pw.printf("%s=%s\n", k, props.get(k) ? props.get(k) : defaultProps.get(k) );
        }
        pw.close()
        return f
    }
}
