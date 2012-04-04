package sys.net.impl;

import java.net.InetSocketAddress;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.NetworkingException;
import sys.net.api.TcpConnection;

/**
 * 
 * Represent a remote location. Used as a destination for sending messages.
 * 
 * @author smd
 * 
 */
public class RemoteEndpoint extends AbstractEndpoint {

	public RemoteEndpoint(final String host, final int tcpPort) {
		super(host, tcpPort);
	}

	RemoteEndpoint(final long locator) {
		super(locator);
	}

	RemoteEndpoint(final InetSocketAddress saddr) {
		super(saddr);
	}

	@Override
	public void setHandler(final MessageHandler handler) {
		throw new NetworkingException("Not supported...[This is a remote endpoint...]");
	}

	@Override
	public TcpConnection send(final Endpoint dst, final Message m) {
		throw new NetworkingException("Not supported...[This is a remote endpoint...]");
	}

	@Override
	public TcpConnection connect(Endpoint dst) {
		throw new NetworkingException("Not supported...[This is a remote endpoint...]");
	}

}
