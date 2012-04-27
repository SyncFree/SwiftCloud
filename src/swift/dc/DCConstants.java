package swift.dc;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class that maintains data-center constants
 * 
 * @author nmp
 * 
 */
public class DCConstants {
    public static final Logger DCLogger = Logger.getLogger("swift.dc");
    public static final int SEQUENCER_PORT = 9998;
    public static final int SURROGATE_PORT = 9999;

    public static final long SYNC_PERIOD = 1000000;

    public static final String DATABASE_CLASS = "DB"; // property for storing
                                                      // the type of database
                                                      // used

    public static final String RIAK_URL = "RIAK:URL"; // property of URL for
                                                      // accessing Riak
    public static final String RIAK_PORT = "RIAK:PORT"; // property of port for
                                                        // accessing Riak

    public static final int DEFAULT_TRXIDTIME = 5000;
    static {
        DCLogger.setLevel(Level.WARNING);
    }
}
