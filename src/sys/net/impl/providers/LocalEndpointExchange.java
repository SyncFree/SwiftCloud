package sys.net.impl.providers;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractTransport;

public class LocalEndpointExchange implements Message {

	Endpoint local;

	public LocalEndpointExchange() {
	}

	public LocalEndpointExchange(Endpoint local) {
		this.local = local;
	}

	public void deliverTo(TransportConnection conn, MessageHandler handler) {
		((AbstractTransport) conn).setRemoteEndpoint(local);
		handler.onAccept(conn);
	}
}