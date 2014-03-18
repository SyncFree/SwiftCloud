package swift.deployment

import static Tools.*

import java.util.List
import java.util.Map
import java.util.concurrent.atomic.AtomicInteger


class SwiftDoc {

    static String SURROGATE_CMD = "-Xincgc -cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties swift.dc.DCServer"
    static String SEQUENCER_CMD = "-Xincgc -cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties swift.dc.DCSequencerServer"

    static String SCOUT_CMD = "-Xincgc -cp swiftcloud.jar -Xincgc -Djava.util.logging.config.file=all_logging.properties swift.application.swiftdoc.SwiftDocBenchmark"

    static String CS_SCOUT_CMD = "-Xincgc -cp swiftcloud.jar -Xincgc -Djava.util.logging.config.file=all_logging.properties swift.application.social.cs.SwiftSocialBenchmarkServer"
    static String CS_ENDCLIENT_CMD = "-Xincgc -cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties swift.application.social.cs.SwiftSocialBenchmarkClient"


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

    static void runEachAsDatacentre( List datacentres, String seqHeap, String surHeap ) {
        println "==== STARTING DATACENTER SERVERS ===="

        int i = 0;
        datacentres.each {
            def srv = it
            def surrogate = srv
            def sequencer = srv
            def other_sequencers = datacentres.clone() - srv
            def name = "X" + i
            rshC(sequencer, swift_app_cmd( "-Xms"+seqHeap, sequencerCmd(name, [srv], other_sequencers), "seq-stdout.txt", "seq-stdout.txt" ))
            rshC(surrogate, swift_app_cmd( "-Xms"+surHeap, surrogateCmd( sequencer ), "sur-stdout.txt", "sur-stderr.txt" ))
            i++;
        }
        println "\nOK"
    }

    static void runStandaloneScouts( List scouts, String server, String iterations, String isolationLevel, String cachepolicy, String notifications, String heap ="512m" ) {
        def cmd = { host ->
            String clientId = 1 + scouts.indexOf( host )
            def res = "nohup java -Xmx" + heap + " " + SCOUT_CMD + " " + server + " " +  iterations + "  " + clientId + "  " + isolationLevel + " " + cachepolicy + " " + notifications
            res += "> scout-stdout.txt 2> scout-stderr.txt < /dev/null &"
            return res;
        }

        AtomicInteger n = new AtomicInteger();
        def resHandler = { host, res ->
            def str = n.incrementAndGet() + "/" + scouts.size() + (res < 1 ? " [ OK ]" : " [FAILED]") + " : " + host
            println str
        }
        Parallel.rsh( scouts, cmd, resHandler, true, 500000)
    }
}