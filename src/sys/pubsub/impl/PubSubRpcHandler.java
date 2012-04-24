package sys.pubsub.impl;

import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class PubSubRpcHandler implements RpcHandler {

    @Override
    public void onReceive(RpcMessage m) {
        Thread.dumpStack();
    }

    @Override
    public void onReceive(RpcConnection conn, RpcMessage m) {
        Thread.dumpStack();
    }

    @Override
    public void onFailure() {
        Thread.dumpStack();
    }

    @Override
    public void onFailure(Endpoint dst, RpcMessage m) {
        Thread.dumpStack();
    }

    public void onReceive(PubSubAck m) {
        Thread.dumpStack();
    }

    public void onReceive(RpcConnection conn, PubSubNotification m) {
        Thread.dumpStack();
    }

}
