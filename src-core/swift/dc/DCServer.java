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

import static swift.dc.DCConstants.DATABASE_CLASS;
import static swift.dc.DCConstants.PRUNE_POLICY;
import static swift.dc.DCConstants.SEQUENCER_PORT;
import static swift.dc.DCConstants.SURROGATE_PORT;
import static swift.dc.DCConstants.SURROGATE_PORT_FOR_SEQUENCERS;
import static sys.net.api.Networking.Networking;

import java.util.List;
import java.util.Properties;

import swift.utils.SafeLog;
import sys.Sys;
import sys.herd.Herd;
import sys.net.api.Endpoint;
import sys.utils.Args;
import sys.utils.Threading;

/**
 * Single server replying to client request. This class is used only to allow
 * client testing.
 * 
 * @author nmp
 * 
 */
public class DCServer {
    DCSurrogate server;
    String sequencerHost;
    Properties props;

    public DCServer(String sequencerHost, Properties props) {
        this.sequencerHost = sequencerHost;
        this.props = props;
        init();
    }

    protected void init() {
    }

    public void startSurrogServer(String siteId, int port4Clients, int port4Sequencers, int portSequencer) {
        Sys.init();

        Endpoint sequencer = Networking.resolve(sequencerHost, portSequencer);

        server = new DCSurrogate(siteId, port4Clients, port4Sequencers, sequencer, props);
    }

    /*
     * For starting two DCs in the same machine use the following parameters:
     * DC1: -sequencer localhost -portSequencer 29996 -portSurrogate 29997
     * -portSurrogateForSequencers 29995 -name S1 DC2: -sequencer localhost
     * -portSequencer 39996 -portSurrogate 39997 -portSurrogateForSequencers
     * 39995 -name S2 Sequencers: SEQ 1: -servers localhost:29995 -name S1 -port
     * 29996 -sequencers localhost:39996 SEQ 2: -servers localhost:39995 -name
     * S1 -port 39996 -sequencers localhost:29996
     */
    public static void main(final String[] args) {

        Args.use(args);

        final Properties props = new Properties();
        props.putAll(System.getProperties());

        String restoreDBdir = Args.valueOf(args, "-rdb", null);
        boolean useBerkeleyDB = Args.contains(args, "-db");

        if (restoreDBdir != null) {
            useBerkeleyDB = true;
            props.setProperty("restore_db", restoreDBdir);
        }

        props.setProperty("sync_commit", Args.contains(args, "-sync") + "");

        if (!props.containsKey(DATABASE_CLASS) || useBerkeleyDB) {
            if (DCConstants.DEFAULT_DB_NULL && !useBerkeleyDB) {
                props.setProperty(DCConstants.DATABASE_CLASS, "swift.dc.db.DevNullNodeDatabase");
            } else {
                props.setProperty(DCConstants.DATABASE_CLASS, "swift.dc.db.DCBerkeleyDBDatabase");
                props.setProperty(DCConstants.BERKELEYDB_DIR, "db/default");
            }
        }
        // TODO: What is that if statement for???
        if (!props.containsKey(DCConstants.DATABASE_CLASS)) {
            // TODO: why should we disable it?
            props.setProperty(DCConstants.PRUNE_POLICY, "false");
        }

        props.setProperty(DCConstants.PRUNING_INTERVAL_PROPERTY,
                Args.valueOf(args, "-pruningMs", DCConstants.DEFAULT_PRUNING_INTERVAL_MS) + "");

        props.setProperty(DCConstants.NOTIFICATION_PERIOD_PROPERTY,
                Args.valueOf(args, "-notificationsMs", DCConstants.DEFAULT_NOTIFICATION_PERIOD_MS) + "");

        String sequencerNode = Args.valueOf(args, "-sequencer", "localhost");
        // int pubsubPort = Args.valueOf(args, "-portPubSub",
        // DCConstants.PUBSUB_PORT);
        int portSequencer = Args.valueOf(args, "-portSequencer", SEQUENCER_PORT);
        int portSurrogate = Args.valueOf(args, "-portSurrogate", SURROGATE_PORT);
        sys.dht.DHT_Node.DHT_PORT = portSurrogate + 2;
        int port4Sequencers = Args.valueOf(args, "-portSurrogateForSequencers", SURROGATE_PORT_FOR_SEQUENCERS);
        final String siteId = Args.valueOf(args, "-name", "X0");

        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-prop:")) {
                props.setProperty(args[i].substring(6), args[++i]);
            } else if (args[i].equals("-prune")) {
                props.setProperty(PRUNE_POLICY, "true");
            }
        }

        List<String> surrogates = Args.subList(args, "-surrogates");
        Herd.setSurrogates(surrogates);

        String shepard = Args.valueOf(args, "-shepard", sequencerNode);
        Herd.setDefaultShepard(shepard);

        if (Args.contains("-integrated"))
            Threading.newThread(true, new Runnable() {
                public void run() {
                    DCSequencerServer.main(args);
                }
            }).start();

        SafeLog.configure(props);

        Threading.sleep(3000);

        new DCServer(sequencerNode, props).startSurrogServer(siteId, portSurrogate, port4Sequencers, portSequencer);
    }
}
