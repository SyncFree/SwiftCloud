package sys.net.impl.rpc;

import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.TcpConnection;
import sys.net.api.rpc.RpcMessage;

public class RpcPayload implements Message {

    public boolean expectingReply;
    public RpcMessage payload;

    RpcPayload() {
    }

    RpcPayload(RpcMessage payload) {
        this(payload, false);
    }

    RpcPayload(RpcMessage payload, boolean expectingReply) {
        this.expectingReply = expectingReply;
        this.payload = payload;
    }

    public void deliverTo(TcpConnection ch, MessageHandler handler) {
        ((RpcEndpointImpl) handler).onReceive(ch, this);
    }
}
