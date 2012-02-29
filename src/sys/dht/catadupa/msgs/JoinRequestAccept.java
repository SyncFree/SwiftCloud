package sys.dht.catadupa.msgs;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class JoinRequestAccept implements RpcMessage {
	
	public JoinRequestAccept(){
	} 
		
	public void deliverTo( RpcConnection sock, RpcHandler handler) {
			((CatadupaHandler) handler).onReceive( this ) ;
	}

}
