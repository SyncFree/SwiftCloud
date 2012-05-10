package sys.dht.catadupa.msgs;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import sys.dht.catadupa.MembershipUpdate;
import sys.dht.catadupa.crdts.time.Timestamp;


public class CatadupaCastPayload implements RpcMessage {

	public Timestamp timestamp;
	public MembershipUpdate data;

	CatadupaCastPayload() {
	}
	
	public CatadupaCastPayload( MembershipUpdate data, Timestamp ts) {
		this.data = data;
		this.timestamp = ts;
	}

	public void deliverTo(RpcHandle call, RpcHandler handler) {
		((CatadupaHandler) handler).onReceive(call, this);
	}
}
