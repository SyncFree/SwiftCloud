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

import sys.Sys;
import sys.herd.Herd;
import sys.net.api.Endpoint;
import sys.utils.Args;

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

    public void startSurrogServer(String siteId, int port4Clients, int port4Sequencers) {
        Sys.init();

        Endpoint sequencer = Networking.resolve(sequencerHost, SEQUENCER_PORT);

        server = new DCSurrogate(siteId, port4Clients, port4Sequencers, sequencer, props);
    }

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.putAll(System.getProperties());
        if (!props.containsKey(DATABASE_CLASS)) {
            if (DCConstants.DEFAULT_DB_NULL) {
                props.setProperty(DCConstants.DATABASE_CLASS, "swift.dc.db.DevNullNodeDatabase");
            } else {
                props.setProperty(DCConstants.DATABASE_CLASS, "swift.dc.db.DCBerkeleyDBDatabase");
                props.setProperty(DCConstants.BERKELEYDB_DIR, "db/default");
            }
        }
        if (!props.containsKey(DCConstants.DATABASE_CLASS)) {
            props.setProperty(DCConstants.PRUNE_POLICY, "false");
        }

        String sequencerNode = Args.valueOf(args, "-sequencer", "localhost");
        int portSurrogate = Args.valueOf(args, "-portSurrogate", SURROGATE_PORT);
        String siteId = Args.valueOf(args, "-name", "X0");

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

        new DCServer(sequencerNode, props).startSurrogServer(siteId, portSurrogate, SURROGATE_PORT_FOR_SEQUENCERS);
    }
}
