package sys.dht;

import static sys.utils.Log.Log;
import sys.dht.api.DHT;
import sys.dht.discovery.Discovery;
import sys.dht.impl.DHT_ClientStub;
import sys.dht.impl.DHT_NodeImpl;
import sys.net.api.Endpoint;
import sys.utils.Threading;

public class DHT_Node extends DHT_NodeImpl {

	public static final String DHT_ENDPOINT = "DHT_ENDPOINT";

	protected DHT_Node() {
		super.init();
	}

	synchronized public static DHT getStub() {
		if (clientStub == null) {
			Endpoint dhtEndpoint = Discovery.lookup(DHT_ENDPOINT, 5000);
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
