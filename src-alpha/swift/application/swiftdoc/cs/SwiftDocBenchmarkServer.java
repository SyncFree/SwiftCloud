package swift.application.swiftdoc.cs;

import java.util.logging.Logger;

import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.dc.DCConstants;
import sys.Sys;

/**
 * 
 * 
 * @author smduarte
 * 
 */
public class SwiftDocBenchmarkServer {

    private static String dcName;
    private static int dcPort;
    private static Logger logger = Logger.getLogger("swift.application.swiftdoc");

    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            System.out.println("Usage: [surrogate address] [server id (1|2)] [isolationLevel] [cachePolicy] [notifications (true|false)]");
            return;
        } else {
            dcName = args[0];
            int pos = dcName.indexOf(":");
            if (pos != -1) {
                dcPort = Integer.parseInt(dcName.substring(pos + 1));
                dcName = dcName.substring(0, pos);
            } else
                dcPort = DCConstants.SURROGATE_PORT;

            int serverId = Integer.parseInt(args[2]);
            SwiftDocServer.isolationLevel = IsolationLevel.valueOf(args[3]);
            SwiftDocServer.cachePolicy = CachePolicy.valueOf(args[4]);
            SwiftDocServer.notifications = Boolean.parseBoolean(args[5]);

            logger.info("Initializing the system");

            Sys.init();

            if (serverId == 1) {
                logger.info("Starting scout/server 1");
                SwiftDocServer.runScoutServer1();
            } else if (serverId == 2) {
                logger.info("Starting scout/server 2");
                SwiftDocServer.runScoutServer2();
            }
        }

    }
}
