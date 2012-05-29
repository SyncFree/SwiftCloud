package sys.dht.test.msgs;

import sys.dht.api.DHT;
import sys.dht.test.KVS;

/**
 * 
 * @author smd
 * 
 */
public class StoreDataReply implements DHT.Reply {

	public String msg;

	/**
	 * Needed for Kryo serialization
	 */
	public StoreDataReply() {
	}

	public StoreDataReply(String reply) {
		this.msg = reply;
	}

	@Override
	public void deliverTo(DHT.Connection conn, DHT.ReplyHandler handler) {
		if (conn.expectingReply())
			((KVS.ReplyHandler) handler).onReceive(conn, this);
		else
			((KVS.ReplyHandler) handler).onReceive(this);
	}

}
