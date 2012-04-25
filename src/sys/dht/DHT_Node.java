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

    public boolean isHandledLocally(final DHT.Key key) {
        return super.resolve( key ).key == self.key;
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

    synchronized public static DHT_Node getInstance() {
        start();
        return singleton;
    }

    private static DHT_Node singleton;
}
