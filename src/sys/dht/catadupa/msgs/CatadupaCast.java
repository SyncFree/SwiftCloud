package sys.dht.catadupa.msgs;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import sys.dht.catadupa.Range;

public class CatadupaCast implements RpcMessage {

	public int level;
	public Range range;
	public long rootKey;
	public CatadupaCastPayload payload;

	CatadupaCast() {
	}

	public CatadupaCast(final int level, final long rootKey, final Range range, final CatadupaCastPayload payload) {
		this.level = level;
		this.payload = payload;
		this.rootKey = rootKey;
		this.range = range.clone();
	}

	public void deliverTo(RpcConnection call, RpcHandler handler) {
		((CatadupaHandler) handler).onReceive(call, this);
	}
}
