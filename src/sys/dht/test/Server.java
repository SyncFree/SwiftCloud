package sys.dht.test;

import java.util.logging.Level;

import sys.dht.DHT_Node;
import sys.dht.api.DHT;
import sys.dht.test.msgs.StoreData;
import sys.dht.test.msgs.StoreDataReply;

import static sys.utils.Log.*;

/**
 * 
 * An example of the server-side of the DHT. As such, this will instantiate a DHT node.
 * 
 * In this example, DHT nodes await requests of (Key + Message(StoreData)).
 * 
 * Any DHT node, upon receiving a request, will forward it, in 1-hop, to the DHT node responsible for the request's key (ie., the sucessor node of the hash of the key).
 * If the client expected a reply, the last DHT node will reply directly, by establishing a new connection to the client node.
 * 
 * Note that to simplify binding, multicast is currently used to discover the endpoint of a seed DHT node.
 * 
 * @author smd (smd@fct.unl.pt)
 * 
 */
public class Server {

    public static void main(String[] args) throws Exception {
        Log.setLevel(Level.ALL);
        sys.Sys.init();

        DHT_Node.start();

        DHT_Node.setHandler(new KVS.RequestHandler() {
            public void onReceive(DHT.Connection conn, DHT.Key key, StoreData request) {
                System.out.printf("Got request for <%s, %s>\n", key, request.data);
                conn.reply(new StoreDataReply("OK"));
            }
        });
    }
}
