package sys.net.impl.rpc;

import sys.net.api.Endpoint;
import sys.net.api.NetworkingException;
import sys.net.api.TcpConnection;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

abstract public class AbstractRpcConnection implements RpcConnection {

	final TcpConnection connection;
	final public boolean expectingReply;

	protected AbstractRpcConnection(TcpConnection conn) {
		this(conn, false);
	}

	protected AbstractRpcConnection(TcpConnection conn, boolean expectingReply) {
		connection = conn;
		this.expectingReply = expectingReply;
	}

	@Override
	public boolean reply(final RpcMessage m) {
		Thread.dumpStack();
		return false;
	}

	@Override
	public boolean reply(final RpcMessage m, final RpcHandler h) {
		Thread.dumpStack();
		return false;
	}

	@Override
	public boolean reply(final RpcMessage m, final RpcHandler h, int timeout) {
		throw new NetworkingException("Not implemented...");
	}

	@Override
	final public boolean expectingReply() {
		return expectingReply;
	}

	@Override
	final public boolean failed() {
		return connection == null || connection.failed();
	}

	@Override
	public void dispose() {
		if (connection != null) {
			connection.dispose();
		}
	}

	@Override
	final public Endpoint remoteEndpoint() {
		return connection != null ? connection.remoteEndpoint() : null;
	}
}
