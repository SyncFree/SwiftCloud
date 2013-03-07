import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;

class Tools {        

    static int DEBUG = 0;
    
    static String USERNAME = "fctple_SwiftCloud"
    static String HOMEDIR = "/home/" + USERNAME + "/"

    static void Debug(int level, msg ) {
        if( DEBUG >= level )
            println msg
    }
    
    static def onControlC = { Closure cloj ->
        boolean invoked = false        
        sun.misc.Signal.handle( new sun.misc.Signal("INT"), [ 
          handle:{ sig ->
          println "\nCaught CONTROL-C "
          if( ! invoked ){
               invoked = true
               cloj.call()
          }
          else System.exit(0)
        } ] as sun.misc.SignalHandler );
     }
        
        
    static File assertFile( String filename ) {
        def File f = new File( filename )
        if( f.exists() ) { 
            return f
        }
        System.err << "ERROR:" << filename << " MISSING"
        System.exit(0);
    }
    
    static dumpTo( targets, filename ) { 
        def pw = new PrintWriter( filename )
        targets.each {
            pw.println( it )
        }
        pw.close()
    }

    static String writeToFile( targets ) {
        File f = File.createTempFile( "swift", ".tmp.txt")
        def pw = new PrintWriter( f )
        targets.each {
            pw.println( it )
        }
        pw.close()
        return f.absolutePath
    }

    
    static Process parallel( List command, List flags, List hosts, List remote_cmd) {
        File f = File.createTempFile( "parallel-", ".txt")
        dumpTo( hosts, f )
        def _cmd = (command + ["-v", "-l", USERNAME, "-h", f.absolutePath] + flags) + remote_cmd
        String str = ""
        _cmd.each { str += it + " "}
        println str
        def cmd2 = ["/bin/sh", "-c", str, "1>&2" ]
        
        Process _proc = new ProcessBuilder( cmd2 ).start()   
        _proc.consumeProcessOutput(System.out, System.out)
        return _proc
    }

    static Process exec( List command ) {
        def _proc = command.execute()
        _proc.consumeProcessOutput(System.out, System.err);
        return _proc
    }
    
    static Process sh( String command ) {
        def _cmd = ["/bin/sh", "-c"] + command;
        Debug(3, _cmd)
        def _proc = _cmd.execute()
        _proc.consumeProcessOutput(System.out, System.err);
        return _proc
    }

    static Process rsh( String host, String command ) {
        def _cmd = ["ssh", USERNAME+"@"+host] + command;
        Debug(3, _cmd)
        return _cmd.execute()
    }

    static Process rshC( String host, String command ) {
        def _proc = rsh( host, command )
        _proc.consumeProcessOutput(System.out, System.err);
        return _proc
    }

    static Process pssh( List hosts, String remote_cmd) {
        return parallel( ["pssh"], ["-i", "-v"], hosts, [remote_cmd] ) ;
    }
    

    static void pnuke( List hosts, String pattern, int timeout) {
        println "KILLALL " + pattern
        AtomicInteger n = new AtomicInteger();
        def cmd = {
            "killall -9 " + pattern
        }
        def resHandler = { host, res ->
            def str = n.incrementAndGet() + "/" + hosts.size() + (res == 0 ? " [ OK ]" : (res == 1 ? " [NO KILL]" : " [FAILED]")) + " : " + host 
            println str
        }
        Parallel.rsh( hosts, cmd, resHandler, true, timeout)
    }
    

    static void prsync( List hosts, String src, String dst, int timeout, boolean verbose = false ) {
        println "RSYNC: " + src + " TO " + dst       
        def cmd = { host ->
            ["rsync", src, String.format("%s@%s:%s", USERNAME, host, dst)]
        }
        AtomicInteger n = new AtomicInteger();
        def resHandler = { host, res ->
            def str = n.incrementAndGet() + "/" + hosts.size() + (res < 1 ? " [ OK ]" : " [FAILED]") + " : " + host
            println str
        }
        Parallel.exec( hosts, cmd, resHandler, verbose, timeout)
    }

    static void pslurp( List hosts, String src, String dstDirPath, String dst, int timeout, boolean verbose = false) {
        println "SLURP: " + src + " TO " + dstDirPath + "/" + dst
        def cmd = { host ->
            File f = new File(dstDirPath + "/" + host + "/");
            f.mkdirs()           
            ["scp", String.format("%s@%s:%s", USERNAME, host, src), f.absolutePath + "/" + dst]
        }

        AtomicInteger n = new AtomicInteger();
        def resHandler = { host, res ->
            def str = n.incrementAndGet() + "/" + hosts.size() + (res < 1 ? " [ OK ]" : " [FAILED]") + " : " + host
            println str
        }
        Parallel.exec( hosts, cmd, resHandler, ! verbose, timeout)
    }

    
    static Process deployTo( List hosts, String filename, boolean verbose = false) {
        return prsync( hosts, filename, filename, 300, ! verbose )
    }
    
    static Process deployTo( List hosts, String src, String dst, boolean verbose = false) {
        return prsync( hosts, src, dst, 300, ! verbose )
    }
    
    static Sleep( int seconds ) {
        Thread.sleep( 1000 * seconds);
    }
    
    static void Countdown( msg, duration ) {
        int seconds = (int)duration
        (seconds..1).each {
            println msg + " " + it + " s"
            sleep(1000)
        }
    }
    
}