package sys.net.impl;


import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.NetworkingException;
import sys.net.api.TransportConnection;
import sys.net.impl.tcp.nio.TcpEndpoint;

/**
 * Represent a local communication endpoint that listens for incomming messagens
 * and allows establishing connections to remote endpoints for supporting
 * message exchanges
 * 
 * @author smd
 * 
 */
public class LocalEndpoint extends AbstractEndpoint {

	final TcpEndpoint tcpEndpoint;

	protected LocalEndpoint(int tcpPort, MessageHandler handler) throws IOException {
		super( handler );		
		tcpEndpoint = new TcpEndpoint(this, tcpPort);
		locator = super.encodeLocator(InetAddress.getLocalHost(), tcpEndpoint.getLocalPort());
		tcpAddress = new InetSocketAddress(InetAddress.getLocalHost(), tcpEndpoint.getLocalPort());
		tcpEndpoint.start();
	}

	static synchronized Endpoint open() {
		return open(0);
	}

	static synchronized Endpoint open(final int tcpPort) {
		return open( tcpPort, null);
	}

	static synchronized Endpoint open(final int tcpPort, MessageHandler handler) {
		try {
			return new LocalEndpoint(tcpPort, handler);
		} catch (IOException x) {
			throw new NetworkingException(x);
		}
	}

	@Override
	public TransportConnection connect(Endpoint dst) {
		return tcpEndpoint.connect( dst );
	}

	@Override
	public TransportConnection send(Endpoint remote, Message m) {		
		return tcpEndpoint.send( remote, m);
	}
}