package sys.dht;

import static sys.dht.catadupa.Config.Config;
import static sys.utils.Log.Log;

import org.hamcrest.Factory;

import sys.dht.api.DHT;
import sys.dht.catadupa.Node;
import sys.dht.discovery.Discovery;
import sys.dht.impl.DHT_ClientStub;
import sys.dht.impl.DHT_NodeImpl;
import sys.net.api.Endpoint;
import sys.pubsub.impl.PubSubService;
import sys.utils.Threading;
import static sys.Sys.*;

public class DHT_Node extends DHT_NodeImpl {

    public static final String DHT_ENDPOINT = "DHT_ENDPOINT";

    protected DHT_Node() {
        super.init();
    }

    public boolean isLocalMatch(final DHT.Key key) {
        long key2key = key.longHashValue() % (1L << Config.NODE_KEY_LENGTH);
        for (Node i : super.db.nodes(key2key))
            if (i.isOnline())
                return i.key == self.key;

        return true;
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

    synchronized public static void start() {
        if (singleton == null) {
            singleton = new DHT_Node();
        }
        while (!singleton.isReady())
            Threading.sleep(50);
    }

    private static DHT_Node singleton;
}
