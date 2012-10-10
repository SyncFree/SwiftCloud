package swift.application.swiftset.cs;

import java.util.Arrays;
import java.util.logging.Logger;

import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import sys.Sys;

/**
 * 
 * 
 * @author smduarte
 * 
 */
public class SwiftSetBenchmarkServer {

    private static Logger logger = Logger.getLogger("swift.application.swiftdoc");

    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            System.err.println(Arrays.asList( args )) ;
            System.err.println("Usage: [surrogate address] [server id (1|2)] [isolationLevel] [cachePolicy] [notifications (true|false)]");
            return;
        } else {
            SwiftSetServer.dcName = args[0];
            
            int serverId = Integer.parseInt(args[1]);
            SwiftSetServer.isolationLevel = IsolationLevel.valueOf(args[2]);
            SwiftSetServer.cachePolicy = CachePolicy.valueOf(args[3]);
            SwiftSetServer.notifications = Boolean.parseBoolean(args[4]);

            logger.info("Initializing the system");

            Sys.init();

            if (serverId == 1) {
                logger.info("Starting scout/server 1");
                SwiftSetServer.runScoutServer1();
            } else if (serverId == 2) {
                logger.info("Starting scout/server 2");
                SwiftSetServer.runScoutServer2();
            }
        }

    }
}
