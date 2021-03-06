/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
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

    public static final int SEQUENCER_PORT = 29996;
    public static final int SURROGATE_PORT = 29997;
        //PUBSUB_PORT = SURROGATE_PORT + 1
        //DhtNode.DHT_PORT = SURROGATE_PORT + 2
//    public static final int PUBSUB_PORT = 30003;
    public static final int SURROGATE_PORT_FOR_SEQUENCERS = 29995;


    public static final long INTERSEQ_RETRY = 5000; // period for retyring
                                                    // re-sending data between
                                                    // sequencers
    public static final long SYNC_PERIOD = 10000; // period for dumping objects
                                                  // to storage

    public static final String DATABASE_CLASS = "DB"; // property for storing
                                                      // the type of database
                                                      // used
    public static final String DATABASE_SYSTEM_TABLE = "SYS_TABLE"; // name of
                                                                    // system
                                                                    // table

    public static final String RIAK_URL = "RIAK_URL"; // property of URL for
                                                      // accessing Riak
    public static final String RIAK_PORT = "RIAK_PORT"; // property of port for
                                                        // accessing Riak
    public static final String BERKELEYDB_DIR = "BERKELEY_DIR"; // directory for
                                                                // storing
                                                                // databases
                                                                // locally

    public static final String PRUNE_POLICY = "prune";

    public static final int DEFAULT_TRXIDTIME = 600000;

    public static final String PRUNING_INTERVAL_PROPERTY = "swift.pruningIntervalMillis";

    public static final int DEFAULT_PRUNING_INTERVAL_MS = 3000;

    public static final boolean DEFAULT_DB_NULL = true;

    public static final String NOTIFICATION_PERIOD_PROPERTY = "swift.notificationPeriodMillis";

    public static final int DEFAULT_NOTIFICATION_PERIOD_MS = 1000;

    public static final String NOTIFICATIONS_SEND_FAKE_PRACTI_DEPOT_VECTORS_PROPERTY = "swift.notificationsFakePracti";

    public static final String DEFAULT_NOTIFICATIONS_SEND_FAKE_PRACTI_DEPOT_VECTORS = "false";

    public static final String NOTIFICATIONS_SEND_DELTA_VECTORS_PROPERTY = "swift.notificationsDeltaVectors";

    public static final String DEFAULT_NOTIFICATIONS_SEND_DELTA_VECTORS = "false";
}
