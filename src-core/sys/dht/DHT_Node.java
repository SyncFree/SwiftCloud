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
package sys.dht;

import static sys.Sys.Sys;

import java.util.Set;
import java.util.logging.Logger;

import sys.dht.api.DHT;
import sys.dht.catadupa.Node;
import sys.dht.discovery.Discovery;
import sys.dht.impl.DHT_ClientStub;
import sys.dht.impl.DHT_NodeImpl;
import sys.net.api.Endpoint;
import sys.utils.Threading;

public class DHT_Node extends DHT_NodeImpl {
    private static Logger Log = Logger.getLogger("sys.dht");

    public static final String DHT_ENDPOINT = "DHT_ENDPOINT";

    protected DHT_Node() {
        super.init();
    }

    public static Set<Long> nodeKeys() {
        return getInstance().odb.nodeKeys();
    }

    static public boolean isHandledLocally(final DHT.Key key) {
        return singleton.resolveNextHop(key).key == singleton.self.key;
    }

    static public Endpoint resolveKey(final DHT.Key key) {
        Node nextHop = singleton.resolveNextHop(key);
        return nextHop.key == singleton.self.key ? null : nextHop.endpoint;
    }

    synchronized public static DHT getStub() {
        if (clientStub == null) {
            String name = DHT_ENDPOINT + Sys.getDatacenter();
            Endpoint dhtEndpoint = Discovery.lookup(name, 5000);
            if (dhtEndpoint != null) {
                clientStub = new DHT_ClientStub(dhtEndpoint);
            } else {
                Log.severe("Failed to discovery DHT access endpoint...");
                return null;
            }
        }
        return clientStub;
    }

    public static void setHandler(DHT.MessageHandler handler) {
        serverStub.setHandler(handler);
    }

    public static void start() {
        if (singleton == null) {
            singleton = new DHT_Node();
        }
        while (!singleton.isReady())
            Threading.sleep(50);
    }

    synchronized public static DHT_Node getInstance() {
        start();
        return singleton;
    }

    private static DHT_Node singleton;
}
