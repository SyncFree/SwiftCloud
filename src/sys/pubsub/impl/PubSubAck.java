package sys.pubsub.impl;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class PubSubAck implements RpcMessage {

    public PubSubAck(){}
    
    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((PubSubRpcHandler)handler).onReceive(this);
    }

}
