package swift.dc;

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

    public static final long INTERSEQ_RETRY = 5000;       //period for retyring re-sending data between sequencers
    public static final long SYNC_PERIOD = 10000;       //period for dumping objects to storage

    public static final String DATABASE_CLASS = "DB"; // property for storing
                                                      // the type of database
                                                      // used
    public static final String DATABASE_SYSTEM_TABLE = "SYS_TABLE"; // name of system table

    public static final String RIAK_URL = "RIAK_URL"; // property of URL for
                                                      // accessing Riak
    public static final String RIAK_PORT = "RIAK_PORT"; // property of port for
                                                        // accessing Riak
    public static final String BERKELEYDB_DIR = "BERKELEY_DIR"; // directory for storing databses locally

    public static final String PRUNE_POLICY = "prune";

    public static final int DEFAULT_TRXIDTIME = 5000;
    
    public static final boolean DEFAULT_DB_NULL = true;
}
