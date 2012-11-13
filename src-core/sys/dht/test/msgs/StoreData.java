package sys.dht.test.msgs;

import sys.dht.api.DHT;
import sys.dht.test.KVS;

/**
 * 
 * @author smd
 * 
 */
public class StoreData implements DHT.Message {

	public Object data;

	/**
	 * Needed for Kryo serialization
	 */
	public StoreData() {
	}

	public StoreData(final Object data) {
		this.data = data;
	}

	public StoreData(String data) {
		this.data = data;
	}

	@Override
	public void deliverTo(DHT.Handle handle, DHT.Key key, DHT.MessageHandler handler) {
		((KVS.RequestHandler) handler).onReceive(handle, key, this);
	}
}
