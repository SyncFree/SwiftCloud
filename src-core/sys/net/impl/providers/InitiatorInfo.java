package sys.net.impl.providers;

import sys.net.api.Endpoint;
import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractMessage;

public class InitiatorInfo extends AbstractMessage {

	protected Endpoint local;

	public InitiatorInfo() {
	}

	public InitiatorInfo(Endpoint local) {
		this.local = local;
	}

	public void deliverTo( TransportConnection conn, MessageHandler handler) {
		((RemoteEndpointUpdater) conn).setRemoteEndpoint(local);
		handler.onAccept(conn);
	}
}