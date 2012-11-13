package sys.net.impl.providers;

import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractMessage;

public class TcpPing extends AbstractMessage  {

	double timestamp;
	
	public TcpPing() {
	}

	public TcpPing( double ts ) {
		this.timestamp = ts;
	}
	
	public void deliverTo( TransportConnection conn, MessageHandler handler) {
		conn.sendNow( new TcpPong( this ) ) ;
	}
}