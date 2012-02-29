package sys.dht.impl;

import sys.dht.api.DHT;
import sys.net.api.Endpoint;

import sys.dht.impl.msgs.DHT_StubHandler;
import sys.dht.impl.msgs.DHT_RequestReply;
import sys.dht.impl.msgs.DHT_Request;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcEndpoint;

import static sys.net.api.Networking.*;

public class DHT_ClientStub implements DHT {

    Endpoint dhtEndpoint;
    RpcEndpoint myEndpoint;

    public DHT_ClientStub(final Endpoint dhtEndpoint) {
        this.dhtEndpoint = dhtEndpoint;
        myEndpoint = Networking.rpcBind(0, new _Handler());
    }

    DHT_ClientStub(final RpcEndpoint myEndpoint, final Endpoint dhtEndpoint) {
        this.myEndpoint = myEndpoint;
        this.dhtEndpoint = dhtEndpoint;
    }

    @Override
    public void send(final Key key, final DHT.Message msg) {
        myEndpoint.send(dhtEndpoint, new DHT_Request(key, msg));
    }

    @Override
    public void send(final Key key, final DHT.Message msg, final DHT.ReplyHandler handler) {
        long handlerId = new DHT_PendingReply(handler).handlerId;
        myEndpoint.send(dhtEndpoint, new DHT_Request(key, msg, handlerId, myEndpoint.localEndpoint()));
    }

    private class _Handler extends DHT_StubHandler {
        protected _Handler() {
        }

        public void onFailure() {
            Thread.dumpStack();
        }

        public void onReceive(final RpcConnection conn, final DHT_RequestReply reply) {
            DHT_PendingReply prh = DHT_PendingReply.getHandler(reply.handlerId);
            if (prh != null) {
                reply.payload.deliverTo(new DHT_ConnectionImpl(conn, reply.replyHandlerId), prh.handler);
            } else {
                Thread.dumpStack();
            }
        }
        
//        public void onReceive(final RpcConnection conn, final DHT_ReplyReply reply) {
//            DHT_PendingReply prh = DHT_PendingReply.getHandler(reply.handlerId);
//            if (prh != null) {
//                reply.payload.deliverTo(new DHT_ConnectionImpl(conn, reply.replyHandlerId), prh.handler);
//            } else {
//                Thread.dumpStack();
//            }
//        }
    }
}
