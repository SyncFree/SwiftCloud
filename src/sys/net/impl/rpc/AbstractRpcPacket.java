package sys.net.impl.rpc;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.TransportConnection;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import sys.net.impl.AbstractMessage;

abstract class AbstractRpcPacket extends AbstractMessage implements Message, RpcHandle, RpcEndpoint, KryoSerializable {

	long handlerId; // destination service handler
	public long replyHandlerId; // reply handler, 0 = no reply expected.
	public int deferredRepliesTimeout = 0;

	RpcMessage payload;

	RpcHandler handler;
	TransportConnection conn;

	int timeout;
	Endpoint remote;
	long timestamp;

	boolean failed = false;
	Throwable failureCause;

	volatile AbstractRpcPacket reply;

	protected AbstractRpcPacket() {
	}

	final public Endpoint remote() {
		return remote != null ? remote : conn.remoteEndpoint();
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

	@Override
	public RpcMessage getPayload() {
		return payload;
	}

	@Override
	public RpcHandle getReply() {
		return reply;
	}

	@Override
	final public void read(Kryo kryo, Input input) {
		this.handlerId = input.readLong();
		this.replyHandlerId = input.readLong();
		this.deferredRepliesTimeout = input.readInt();
		
		this.payload = (RpcMessage) kryo.readClassAndObject(input);
	}

	@Override
	final public void write(Kryo kryo, Output output) {
		output.writeLong(this.handlerId);
		output.writeLong(this.replyHandlerId);
		output.writeInt( deferredRepliesTimeout);
		
		kryo.writeClassAndObject(output, payload);
	}
}