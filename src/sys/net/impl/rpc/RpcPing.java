package sys.net.impl.rpc;

import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractMessage;

public class RpcPing extends AbstractMessage  {
	
	public double timestamp;

	public RpcPing() {		
	}
	
	public RpcPing( double ts ) {
		this.timestamp = ts;
	}
		
	@Override
	public void deliverTo(TransportConnection conn, MessageHandler handler) {
		((RpcFactoryImpl) handler).onReceive(conn, this);
	}	
}
