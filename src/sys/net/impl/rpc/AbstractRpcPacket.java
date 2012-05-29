package sys.net.impl.rpc;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import sys.net.impl.rpc.RpcFactoryImpl.RpcPacket;

public class AbstractRpcPacket implements Message, RpcHandle, RpcEndpoint {

	long handlerId; // destination service handler
	long replyHandlerId; // reply handler, 0 = no reply expected.
	RpcMessage payload;

	RpcHandler handler;
	TransportConnection conn;

	int timeout;
	Endpoint remote;
	long timestamp;

	boolean failed = false;
	Throwable failureCause;
	boolean streamingIsEnabled = false;

	volatile RpcPacket reply;

	public AbstractRpcPacket() {
	}

	public Endpoint remote() {
		return remote;
	}

	@Override
	public boolean expectingReply() {
		return replyHandlerId != 0;
	}

	@Override
	public RpcHandle reply(RpcMessage msg) {
		return reply(msg, null, 0);
	}

	@Override
	public RpcHandle reply(RpcMessage msg, RpcHandler handler) {
		return reply(msg, handler, -1);
	}

	@Override
	public RpcHandle reply(RpcMessage msg, RpcHandler handler, int timeout) {
		Thread.dumpStack();
		return null;
	}

	@Override
	public boolean failed() {
		return failed;
	}

	@Override
	public boolean succeeded() {
		return !failed;
	}

	@Override
	public Endpoint remoteEndpoint() {
		return remote == null ? conn.remoteEndpoint() : remote;
	}

	@Override
	public Endpoint localEndpoint() {
		return null;
	}

	@Override
	public RpcHandle send(Endpoint dst, RpcMessage m) {
		return send(dst, m, null, 0);
	}

	@Override
	public RpcHandle send(Endpoint dst, RpcMessage m, RpcHandler replyHandler) {
		return send(dst, m, replyHandler, -1);
	}

	@Override
	public RpcHandle send(Endpoint dst, RpcMessage m, RpcHandler replyHandler, int timeout) {
		Thread.dumpStack();
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends RpcEndpoint> T setHandler(final RpcHandler handler) {
		this.handler = handler;
		return (T) this;
	}

	public void deliverTo(TransportConnection conn, MessageHandler handler) {
	}

	@Override
	public void enableStreamingReplies(boolean flag) {
		streamingIsEnabled = flag;
	}

	@Override
	public RpcMessage getPayload() {
		return payload;
	}

	@Override
	public RpcHandle getReply() {
		return reply;
	}
}