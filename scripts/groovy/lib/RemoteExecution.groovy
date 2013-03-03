import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

class RemoteExecution {
    static String ACCOUNT = "fctple_SwiftCloud"
    
    static Executor threads = Executors.newCachedThreadPool();
    
    static void rsh( List<String> hosts, Closure cmd,  boolean consumeIO, int timeout ) {
        
        final Set<String> failed = Collections.synchronizedSet( new HashSet<String>() )
        final Set<String> succeeded = Collections.synchronizedSet( new HashSet<String>() )
        
        hosts.each { String it ->
            threads.execute( new Runnable() {
                public void run() {
                    Process proc = remoteExec( it, cmd, consumeIO )
                    proc.waitFor();
                    int result = proc.exitValue()
                    if( result < 0 ) {
                        failed.add( it )
                    }
                    else {
                        println it
                        succeeded.add( it )
                        notifyAllOn( succeeded )
                    }
                }
            });
        }
        
        long deadline = System.currentTimeMillis() + timeout * 1000
        while( ( (succeeded.size() + failed.size()) != hosts.size() ) && System.currentTimeMillis() < deadline) {
            waitOn(succeeded, Math.min(1000, deadline - System.currentTimeMillis()))
            System.err.printf("\rOK: %s/%s)\n", succeeded.size(), hosts.size())
        }
        println "ALL DONE"
    }
    
    
    static public Process remoteExec( String target, Closure cmd, boolean consumeIO ) {
        System.err.println( target );
        List<String> rcmd = new ArrayList<String>()
        
        rcmd.add("ssh")
        rcmd.add( String.format("%s@%s", ACCOUNT, target))        
        rcmd.add( cmd.call( target ) )

        Process proc = rcmd.execute();
        if( consumeIO)
            proc.consumeProcessOutput( System.out, System.err)
        
        return proc
    }
    
    
    static void dumpIO(final InputStream ins, final OutputStream out) {
        threads.execute(new Runnable() {
            public void run() {
                try {
                    int n;
                    byte[] tmp = new byte[1024];
                    while ((n = ins.read(tmp)) > 0)
                        out.write(tmp, 0, n);

                } catch (IOException x) {
                }
            }
        });
    }
    
    static void waitOn( Object obj, long timeout ) {
        synchronized( obj ) {
            try {
                obj.wait( timeout );
            } catch( Exception x ) {}
        }
    }

    static void notifyAllOn( Object obj) {
        synchronized( obj ) {
            try {
                obj.notifyAll();
            } catch( Exception x ) {}
        }
    }
}
    