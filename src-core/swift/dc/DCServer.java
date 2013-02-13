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

import static sys.net.api.Networking.Networking;

import java.util.Properties;

import sys.Sys;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;

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

    public void startSurrogServer(int port4Clients, int port4Sequencers) {
        Sys.init();

        RpcEndpoint srvEP4Clients = Networking.rpcBind(port4Clients).toDefaultService();
        RpcEndpoint srvEP4Sequencer = Networking.rpcBind(port4Sequencers).toDefaultService();
        RpcEndpoint cltEP4Sequencer = Networking.rpcConnect().toDefaultService();

        Endpoint sequencer = Networking.resolve(sequencerHost, DCConstants.SEQUENCER_PORT);

        server = new DCSurrogate(srvEP4Clients, srvEP4Sequencer, cltEP4Sequencer, sequencer, props);
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.putAll(System.getProperties());
        if (!props.containsKey(DCConstants.DATABASE_CLASS)) {
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
        int portSurrogate = DCConstants.SURROGATE_PORT;

        String sequencerNode = "localhost";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-sequencer")) {
                sequencerNode = args[++i];
            } else if (args[i].startsWith("-prop:")) {
                props.setProperty(args[i].substring(6), args[++i]);
            } else if (args[i].equals("-portSurrogate")) {
                portSurrogate = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-prune")) {
                props.setProperty(DCConstants.PRUNE_POLICY, "true");
            }
        }

        new DCServer(sequencerNode, props).startSurrogServer(portSurrogate, DCConstants.SURROGATE_PORT_FOR_SEQUENCERS);
    }
}
