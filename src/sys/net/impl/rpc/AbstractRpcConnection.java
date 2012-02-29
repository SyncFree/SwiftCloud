package sys.net.impl.rpc;

import sys.net.api.Endpoint;
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
        this.connection = conn;
        this.expectingReply = expectingReply;
    }

    public boolean reply(final RpcMessage m) {
        Thread.dumpStack();
        return false;
    }

    public boolean reply(final RpcMessage m, final RpcHandler h) {
        Thread.dumpStack();
        return false;
    }

    final public boolean expectingReply() {
        return expectingReply;
    }

    final public boolean failed() {
        return connection == null || connection.failed();
    }

    final public void dispose() {
        if (connection != null) {
            connection.dispose();
        }
    }

    final public Endpoint remoteEndpoint() {
        return connection != null ? connection.remoteEndpoint() : null;
    }
}
