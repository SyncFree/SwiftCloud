package sys.net.impl.rpc;

import sys.net.api.NetworkingException;
import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.TcpConnection;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class RpcEndpointImpl implements RpcEndpoint, MessageHandler {

    RpcHandler handler;
    public Endpoint endpoint;
    RpcHandler failureHandler;

    public RpcEndpointImpl(Endpoint endpoint, RpcHandler handler) {
        this.handler = handler;
        this.endpoint = endpoint;
        this.endpoint.setHandler(this);
        this.failureHandler = handler;
    }

    public void setHandler(RpcHandler handler) {
        this.handler = handler;
        this.failureHandler = handler;
    }

    public Endpoint localEndpoint() {
        return endpoint;
    }

    public boolean send(final Endpoint dst, final RpcMessage m) {
        if (new OutgoingRPC_ExpectingNoReply(dst, m).connection.failed()) {
            if (failureHandler != null)
                failureHandler.onFailure(dst, m);
            return false;
        } else
            return true;
    }

    public boolean send(final Endpoint dst, final RpcMessage m, final RpcHandler replyHandler) {
        if (new OutgoingRPC_ExpectingReply(dst, m, replyHandler).connection.failed()) {
            replyHandler.onFailure();
            if (failureHandler != null)
                failureHandler.onFailure(dst, m);
            return false;
        }
        return true;
    }

    public void onReceive(final TcpConnection conn, final RpcPayload m) {
        if (m.expectingReply)
            new IncomingRPC_ExpectingReply(conn, m.payload);
        else
            new IncomingRPC_ExpectingNoReply(conn, m.payload);
    }

    public String toString() {
        return "" + handler;
    }

    class OutgoingRPC_ExpectingNoReply {

        TcpConnection connection;

        OutgoingRPC_ExpectingNoReply(final Endpoint dst, final RpcMessage msg) {
            connection = endpoint.send(dst, new RpcPayload(msg));
            if (connection != null) {
                connection.dispose();
            }
        }
    }

    class OutgoingRPC_ExpectingReply {

        TcpConnection connection;

        OutgoingRPC_ExpectingReply(final Endpoint dst, final RpcMessage msg, final RpcHandler replyHandler) {
            RpcPayload reply;
            connection = endpoint.send(dst, new RpcPayload(msg, true));
            if (!connection.failed() && (reply = connection.receive()) != null) {
                if (reply.expectingReply) {
                    new IncomingRPC_ExpectingReply(connection, reply.payload, replyHandler);
                } else {
                    new IncomingRPC_ExpectingNoReply(connection, reply.payload, replyHandler);
                }
            }
        }
    }

    class IncomingRPC_ExpectingNoReply extends AbstractRpcConnection {

        IncomingRPC_ExpectingNoReply(TcpConnection connection, RpcMessage payload) {
            super(connection);
            payload.deliverTo(this, handler);
            connection.dispose();
        }

        IncomingRPC_ExpectingNoReply(TcpConnection connection, RpcMessage payload, RpcHandler handler) {
            super(connection);
            payload.deliverTo(this, handler);
            connection.dispose();
        }
    }

    class IncomingRPC_ExpectingReply extends AbstractRpcConnection {

        IncomingRPC_ExpectingReply(TcpConnection channel, RpcMessage payload) {
            super(channel, true);
            payload.deliverTo(this, handler);
        }

        IncomingRPC_ExpectingReply(TcpConnection channel, RpcMessage payload, RpcHandler handler) {
            super(channel, true);
            payload.deliverTo(this, handler);
        }

        @Override
        public boolean reply(final RpcMessage m) {
            connection.send(new RpcPayload(m));
            connection.dispose();
            return !connection.failed();
        }

        @Override
        public boolean reply(final RpcMessage m, final RpcHandler replyHandler) {
            connection.send(new RpcPayload(m, true));
            RpcPayload rpc = connection.receive();
            if (rpc != null) {
                if (rpc.expectingReply)
                    new IncomingRPC_ExpectingReply(connection, rpc.payload, replyHandler);
                else
                    new IncomingRPC_ExpectingNoReply(connection, rpc.payload, replyHandler);
            } else {
                connection.dispose();
            }
            if (connection.failed()) {
                replyHandler.onFailure();
                return false;
            } else {
                return true;
            }
        }
    }

    @Override
    public void onReceive(TcpConnection conn, Message m) {
        throw new NetworkingException("Incoming object is not an RpcMessage");
    }

    public void onFailure(Endpoint dst, Message m) {
    }
}