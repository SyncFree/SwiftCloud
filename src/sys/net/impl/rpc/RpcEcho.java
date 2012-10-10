package sys.net.impl.rpc;


import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractMessage;

public class RpcEcho extends AbstractMessage {
	
	public RpcEcho() {			    
	}
			
	@Override
	public void deliverTo(TransportConnection conn, MessageHandler handler) {
		((RpcEchoHandler) handler).onReceive(conn, this);
	}
}
