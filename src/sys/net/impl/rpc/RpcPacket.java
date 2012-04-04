package sys.net.impl.rpc;

import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.TcpConnection;
import sys.net.api.rpc.RpcMessage;

public class RpcPacket implements Message {

	long handlerId; // destination service handler
	long replyHandlerId; // reply handler, 0 = no reply expected.

	RpcMessage payload;

	RpcPacket() {
	}

	RpcPacket(RpcMessage payload, long handlerId, long replyHandlerId) {
		this.payload = payload;
		this.handlerId = handlerId;
		this.replyHandlerId = replyHandlerId;
	}

	@Override
	public void deliverTo(TcpConnection ch, MessageHandler handler) {
		((RpcFactoryImpl) handler).onReceive(ch, this);
	}
}