package sys.dht.catadupa.msgs;

import sys.dht.catadupa.Node;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class JoinRequest implements RpcMessage {
	
	public Node node ;
	
	public JoinRequest(){}
	
	public JoinRequest( final Node self){	
		this.node = self;
	}
	
	public void deliverTo( RpcConnection call, RpcHandler handler) {
		((CatadupaHandler) handler).onReceive(call, this);
	}
	
}