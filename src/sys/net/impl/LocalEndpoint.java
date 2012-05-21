package sys.net.impl;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.NetworkingException;
import sys.net.api.TransportConnection;

/**
 * Represent a local communication endpoint that listens for incomming messagens
 * and allows establishing connections to remote endpoints for supporting
 * message exchanges
 * 
 * @author smd
 * 
 */
public class LocalEndpoint extends AbstractEndpoint {

	final AbstractLocalEndpoint provider;

	protected LocalEndpoint(int tcpPort, MessageHandler handler, TransportProvider providerType ) throws Exception {
		super( handler );		
		switch( providerType ) {
			case NETTY_IO_WS:
				provider = new sys.net.impl.providers.netty.ws.WebSocketEndpoint(this, tcpPort);
			break;
			case NETTY_IO_TCP:
				provider = new sys.net.impl.providers.netty.tcp.TcpEndpoint(this, tcpPort);
				break;
			case DEFAULT:
			case NIO_TCP:
			default:
				provider = new sys.net.impl.providers.nio.TcpEndpoint(this, tcpPort);
			break;
		}
		locator = super.encodeLocator(InetAddress.getLocalHost(), provider.getLocalPort());
		tcpAddress = new InetSocketAddress(InetAddress.getLocalHost(), provider.getLocalPort());
		provider.start();
	}


	static synchronized Endpoint open(final int tcpPort, MessageHandler handler, TransportProvider provider) {
		try {
			return new LocalEndpoint(tcpPort, handler, provider);
		} catch (Exception x) {
			throw new NetworkingException(x);
		}
	}
	
	@Override
	public TransportConnection connect(Endpoint dst) {
		return provider.connect( dst );
	}

	@Override
	public TransportConnection send(Endpoint remote, Message m) {		
		return provider.send( remote, m);
	}
}