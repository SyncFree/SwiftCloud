package swift.application.swiftdoc.cs;

import java.util.Arrays;
import java.util.logging.Logger;

import sys.Sys;

/**
 * 
 * 
 * @author smduarte
 * 
 */
public class SwiftDocBenchmarkClient {

    private static int clientId;
    private static String scoutName;
    private static Logger logger = Logger.getLogger("swift.application.swiftdoc.client");

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("-->" + Arrays.asList(args));
            System.out.println("Usage: [scout address] [client id (1|2)]");
            return;
        } else {
            scoutName = args[0];
            clientId = Integer.parseInt(args[1]);

            logger.info("Initializing the system");

            Sys.init();

            if (clientId == 1) {
                logger.info("Starting client 1");
                SwiftDocClient.runClient1Code( scoutName );
            } else if (clientId == 2) {
                logger.info("Starting client 2");
                SwiftDocClient.runClient1Code( scoutName );
            }
        }
    }
}
