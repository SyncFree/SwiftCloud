package swift.application.swiftdoc.cs;

import java.util.Arrays;
import java.util.logging.Logger;

import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;

/**
 * 
 * 
 * @author smduarte
 * 
 */
public class SwiftDocBenchmarkClient {

    private static int clientId;
    private static String scoutName, dcName;
    private static Logger logger = Logger.getLogger("swift.application.swiftdoc.client");

    public static void main(String[] args) throws Exception {

        if (false && args.length != 3) {
            System.out.println("-->" + Arrays.asList(args));
            System.out.println("Usage: [scout address] [client id (1|2)] [DC address]");
            return;
        } else {
            args = new String[] {"localhost", "1", "localhost"};
            
            scoutName = args[0];
            clientId = Integer.parseInt(args[1]);
            dcName = args[2];
            System.err.println( dcName );
            logger.info("Initializing the system");

            Sys.init();
            DCSequencerServer.main(new String[]{"-name", "localhost"});

            // start DC server
            DCServer.main(new String[]{"localhost"});

            if (clientId == 1) {
                logger.info("Starting client 1");
                SwiftDocClient2.runClient1Code( scoutName, dcName );
            } else if (clientId == 2) {
                logger.info("Starting client 2");
                SwiftDocClient2.runClient2Code( scoutName );
            }
        }
    }
}
