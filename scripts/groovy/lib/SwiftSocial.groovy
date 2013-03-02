
import static Tools.*

class SwiftSocial {

    static String SHEPARD_CMD = "-cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties sys.shepard.Shepard"
    
    static String SURROGATE_CMD = "-cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties swift.dc.DCServer"
    static String SEQUENCER_CMD = "-cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties swift.dc.DCSequencerServer"    

    static String INITDB_CMD = "-cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties swift.application.social.SwiftSocialBenchmark"
    
    
    static String SCOUT_CMD = "-cp swiftcloud.jar -Xincgc -Djava.util.logging.config.file=all_logging.properties swift.application.social.SwiftSocialBenchmark"

    static String CS_SCOUT_CMD = "-cp swiftcloud.jar -Xincgc -Djava.util.logging.config.file=all_logging.properties swift.application.social.cs.SwiftSocialBenchmarkServer"
    static String CS_ENDCLIENT_CMD = "-cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties swift.application.social.cs.SwiftSocialBenchmarkClient"
    
    
    static String swift_app_cmd( String heap, String exec ) {
        return "java " + heap + " " + exec + "2> >(tee stderr.txt 1>&2) > >(tee stdout.txt)"
    }
    
    static String swift_app_cmd_nostdout( String heap, String exec )  {
        return "java " + heap + " " + exec +  "2> >(tee stderr.txt 1>&2) > stdout.txt"
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
        Process proc = rsh( host, swift_app_cmd("-Xmx64m", cmd) ); 
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
            rshC(sequencer, swift_app_cmd( "-Xms"+seqHeap, sequencerCmd(name, [srv], other_sequencers) ))
            rshC(surrogate, swift_app_cmd( "-Xms"+surHeap, surrogateCmd( sequencer ) ))            
            i++;
        }
        println "\nOK"
    }
    
    
    static void initDB( String client, String server, String config ) {
        println "CLIENT: " + client + " SERVER: " + server + " CONFIG: " + config
        
        def cmd = "-Dswiftsocial=" + config + " " + INITDB_CMD + " init " + server + " "      
        rshC( client, swift_app_cmd("-Xmx128m", cmd)).waitFor();
        println "OK.\n"
    }  
    
    
    static void runStandaloneScouts( List scouts, List servers, String config, String shepard, int threads ) {        
        def cmd = "nohup java -Xmx512m -Dswiftsocial=" + config + " " + SCOUT_CMD + " run " + shepard + " " + threads + " scouts.txt -servers "
        servers.each { cmd += it + " "}     
        cmd += "> stdout.txt 2> stderr.txt < /dev/null &"
        pssh( scouts, cmd).waitFor()
    }
    
    
    static void runCS_ServerScouts( List scouts, List servers, String config, String cache) {
        def cmd = "nohup java -Xmx512m -Dswiftsocial=" + config + " " + CS_SCOUT_CMD + " " + cache + " -servers "
        servers.each { cmd += it + " "}     
        cmd += "> scout-stdout.txt 2> scout-stderr.txt < /dev/null &"
//        Debug(0, cmd)
        pssh( scouts, cmd).waitFor()
    }
    
    static void runCS_EndClients( List clients, List scouts, String config, String shepard, int threads) {
        def cmd = "nohup java -Xmx128m -Dswiftsocial=" + config + " " + CS_ENDCLIENT_CMD + " -shepard " + shepard + " -threads " + threads + " -servers "  
        scouts.each { cmd += it + " "}     
        cmd += "> client-stdout.txt 2> client-stderr.txt < /dev/null &"
        Debug(0, cmd)
        pssh( clients, cmd).waitFor()
    }
}