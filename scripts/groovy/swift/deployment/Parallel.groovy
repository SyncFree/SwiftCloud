package swift.deployment
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor
import java.util.concurrent.Executors

class Parallel {
    static String ACCOUNT = "fctple_SwiftCloud"

    static Executor threads = Executors.newFixedThreadPool(48)

    static Map rsh( List<String> hosts, Closure cmd,  Closure resHandler, boolean ignoreIO, int timeout ) {
        def res = new ConcurrentHashMap<String, Process>()

        hosts.each { String it ->
            threads.execute( new Runnable() {
                        public void run() {
                            def cmdline = [
                                "ssh",
                                String.format("%s@%s", ACCOUNT, it),
                                cmd.call(it)
                            ]
                            
                            println cmdline
                            
                            Process proc = new ProcessBuilder(cmdline).redirectErrorStream(true).start()
                            Thread.startDaemon {
                                proc.inputStream.eachLine { if( ! ignoreIO ) println it }
                            }
                            proc.waitFor();
                            res[it] = proc
                            int result = proc.exitValue()
                            resHandler.call( it, result );
                            notifyAllOn( res )
                        }
                    });
        }

        long deadline = System.currentTimeMillis() + timeout * 1000
        while( ( res.size() != hosts.size() ) && System.currentTimeMillis() < deadline)
            waitOn(res, Math.min(1000, deadline - System.currentTimeMillis()))

        hosts.each {
            Process p = res[it]
            if( p == null)
                resHandler.call( it, -1 )
        }
        return res
    }

    static Map exec( List<String> hosts, Closure cmd,  Closure resHandler, boolean discardIO, int timeout ) {
        def res = new ConcurrentHashMap<String, Process>()

        hosts.each { String it ->
            threads.execute( new Runnable() {
                        public void run() {
                            def cmdline = cmd.call(it)
                            Process proc = new ProcessBuilder(cmdline).redirectErrorStream(true).start()
                            Thread.startDaemon {
                                proc.inputStream.eachLine { if( ! discardIO ) println it }
                            }
                            proc.waitFor();
                            res[it] = proc
                            int result = proc.exitValue()
                            resHandler.call( it, result );
                            notifyAllOn( res )
                        }
                    });
        }

        long deadline = System.currentTimeMillis() + timeout * 1000
        while( ( res.size() != hosts.size() ) && System.currentTimeMillis() < deadline)
            waitOn(res, Math.min(1000, deadline - System.currentTimeMillis()))

        hosts.each {
            Process p = res[it]
            if( p == null)
                resHandler.call( it, -1 )
        }
        return res
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
            } catch( Exception x ) {
            }
        }
    }

    static void notifyAllOn( Object obj) {
        synchronized( obj ) {
            try {
                obj.notifyAll();
            } catch( Exception x ) {
            }
        }
    }
}
