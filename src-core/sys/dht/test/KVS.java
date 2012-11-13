package sys.dht.test;

import sys.dht.api.DHT;
import sys.dht.test.msgs.StoreData;
import sys.dht.test.msgs.StoreDataReply;

/**
 * 
 * The KVS interface used in this example, split into its client/server sides...
 * 
 * @author smd
 * 
 */
public interface KVS {

	/**
	 * Denotes collection of requests/messages that the KVS server
	 * processes/expects
	 * 
	 * @author smd
	 * 
	 */
	abstract class RequestHandler extends DHT.AbstractMessageHandler {
		abstract public void onReceive(DHT.Handle handle, DHT.Key key, StoreData request);
	}

	/**
	 * Denotes collection of reply/messages that the KVS client processes
	 * 
	 * @author smd
	 * 
	 */
	abstract class ReplyHandler extends DHT.AbstractReplyHandler {
		abstract public void onReceive(StoreDataReply reply);
	}
}
