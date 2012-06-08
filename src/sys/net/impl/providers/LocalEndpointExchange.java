package sys.net.impl.providers;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;

public class LocalEndpointExchange implements Message {

	protected Endpoint local;

	public LocalEndpointExchange() {
	}

	public LocalEndpointExchange(Endpoint local) {
		this.local = local;
	}

	public void deliverTo( TransportConnection conn, MessageHandler handler) {
		((RemoteEndpointUpdater) conn).setRemoteEndpoint(local);
		handler.onAccept(conn);
	}
}