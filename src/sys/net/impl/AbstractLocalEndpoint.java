package sys.net.impl;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.TransportConnection;

public abstract class AbstractLocalEndpoint extends AbstractEndpoint {

	protected Endpoint localEndpoint;

	abstract public void start() throws Exception;

	abstract public TransportConnection connect(Endpoint dst);

	@Override
	public TransportConnection send(Endpoint remote, Message m) {
		TransportConnection conn = connect(remote);
		if (conn != null && conn.send(m))
			return conn;
		else
			return null;
	}

	protected long getLocator(SocketAddress addr) {
		InetSocketAddress iaddr = (InetSocketAddress) addr;
		return ByteBuffer.wrap(iaddr.getAddress().getAddress()).getInt() << 32 | iaddr.getPort();
	}
}
