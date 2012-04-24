package sys.pubsub.impl;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class PubSubAck implements RpcMessage {

    int totalSubscribers;
    
    PubSubAck(){}

    public PubSubAck( int subscribers ){
        this.totalSubscribers = subscribers;
    }

    public int totalSubscribers() {
        return totalSubscribers;
    }
    
    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((PubSubRpcHandler)handler).onReceive( conn, this);
    }

}
