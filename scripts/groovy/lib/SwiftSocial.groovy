
import static Tools.*

import java.util.List
import java.util.concurrent.atomic.AtomicInteger


class SwiftSocial {

    static String SHEPARD_CMD = "-cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties sys.shepard.Shepard"
    
    static String SURROGATE_CMD = "-Xincgc -cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties swift.dc.DCServer"
    static String SEQUENCER_CMD = "-Xincgc -cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties swift.dc.DCSequencerServer"    

    static String INITDB_CMD = "-cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties swift.application.social.SwiftSocialBenchmark"
    
    
    static String SCOUT_CMD = "-Xincgc -cp swiftcloud.jar -Xincgc -Djava.util.logging.config.file=all_logging.properties swift.application.social.SwiftSocialBenchmark"

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
    
    
    static void initDB( String client, String server, String config, String heap = "512m") {
        println "CLIENT: " + client + " SERVER: " + server + " CONFIG: " + config
        
        def cmd = "-Dswiftsocial=" + config + " " + INITDB_CMD + " init -servers " + server + " "      
        rshC( client, swift_app_cmd("-Xmx" + heap, cmd, "initdb-stdout.txt", "initdb-stderr.txt")).waitFor();
        println "OK.\n"
    }  
    
    
    static void runStandaloneScouts( List scouts, List servers, String config, String shepard, int threads, String heap ="512m" ) {        
        def cmd = { host -> 
            String partition = scouts.indexOf( host ) + "/" + scouts.size()
            def res = "nohup java -Xmx" + heap + " -Dswiftsocial=" + config + " " + SCOUT_CMD + " run -shepard " + shepard + " -threads " + threads + " -partition " + partition + " -servers "
            servers.each { res += it + " "}     
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

        static void runCS_ServerScouts( int instances, List scouts, List servers, String config, String heap="512m") {
        
        AtomicInteger n = new AtomicInteger();
        def resHandler = { host, res ->
            def str = n.incrementAndGet() + "/" + scouts.size() + (res < 1 ? " [ OK ]" : " [FAILED]") + " : " + host
            println str
        }
        
        instances.times{ instance ->
            def cmd = { _ ->
                def str = "nohup nice -n 10 java -Xmx" + heap + " -Dswiftsocial=" + config + " " + CS_SCOUT_CMD + " -instance " + instance + " -servers "
                servers.each { str += it + " "}
                str += "> scout-stdout-"+ instance+".txt 2> scout-stderr-"+instance+".txt < /dev/null &"
            }
            Parallel.rsh( scouts, cmd, resHandler, true, 500000)         
        }
    }
    
    static void runCS_EndClients( int instances, List clients, List scouts, String config, String shepard, int threads, String heap = "128m") {
        def cmd = { host -> 
                String partition = clients.indexOf( host ) + "/" + clients.size()
                def res = "nohup java -Xmx" + heap + " -Dswiftsocial=" + config + " " + CS_ENDCLIENT_CMD + " -shepard " + shepard + " -threads " + threads + " -partition " + partition + " -instances " + instances + " -servers "  
                scouts.each { res += it + " "}     
                res += "> client-stdout.txt 2> client-stderr.txt < /dev/null &"
                return res
        }

        AtomicInteger n = new AtomicInteger();
        def resHandler = { host, res ->
            def str = n.incrementAndGet() + "/" + clients.size() + (res < 1 ? " [ OK ]" : " [FAILED]") + " : " + host
            println str
        }
        Parallel.rsh( clients, cmd, resHandler, true, 500000) 
    }
}