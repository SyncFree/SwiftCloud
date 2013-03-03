import java.util.concurrent.SynchronousQueue;

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
        def _cmd = (command + ["-l", USERNAME, "-h", f.absolutePath] + flags) + remote_cmd
        def _proc = _cmd.execute()
        Debug(3, _cmd)
        _proc.consumeProcessOutput(System.out, System.err);
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
    

        
    
    static Process pnuke( List hosts, String pattern) {
        println "KILLALL " + pattern
        
        def proc = parallel( ["pnuke"], [], hosts, [pattern] )
        proc.waitFor()
        return proc
    }
    
    static Process pslurp( List hosts, String src, String dst, String prefix) {
        println "SLURP: " + src + " TO " + dst
        new File(prefix).mkdirs()
        return parallel( ["pslurp"], ["-L", prefix, src, dst], hosts, [] ) ;
    }

    static Process prsync( List hosts, String src, String dst) {
        println "RSYNC: " + src + " TO " + dst       
        return parallel( ["prsync"], ["-p", "10", src, dst], hosts, [] ) ;
    }
    
    static Process deployTo( List hosts, String filename) {
        return prsync( hosts, filename, HOMEDIR + filename )
    }
    
    static Process deployTo( List hosts, String src, String dst) {
        return prsync( hosts, src, HOMEDIR + dst )
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