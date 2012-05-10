package sys.dht.impl;

import sys.dht.api.DHT;
import sys.dht.impl.msgs.DHT_RequestReply;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcEndpoint;

class DHT_ConnectionImpl implements DHT.Connection {

	final Endpoint dst;
	final long handleId;
	final RpcHandle conn;
	final RpcEndpoint dhtEndpoint;

	DHT_ConnectionImpl(RpcHandle conn, long handleId) {
		this(conn, handleId, null, null);
	}

	DHT_ConnectionImpl(RpcHandle conn, long handleId, RpcEndpoint dhtEndpoint, Endpoint dst) {
		this.dst = dst;
		this.conn = conn;
		this.handleId = handleId;
		this.dhtEndpoint = dhtEndpoint;
	}

	@Override
	public boolean expectingReply() {
		return handleId > 0;
	}

	@Override
	public boolean reply(DHT.Reply msg) {
		if (dhtEndpoint == null)
			return conn.reply(new DHT_RequestReply(msg, handleId)).succeeded();
		else
			return dhtEndpoint.send(dst, new DHT_RequestReply(msg, handleId)).succeeded();
	}

	@Override
	public boolean reply(DHT.Reply msg, DHT.ReplyHandler handler) {
		if (dhtEndpoint == null)
			return conn.reply(new DHT_RequestReply(msg, handleId, new DHT_PendingReply(handler).handlerId)).succeeded();
		else
			return dhtEndpoint.send(dst, new DHT_RequestReply(msg, handleId, new DHT_PendingReply(handler).handlerId)).succeeded();
	}

	@Override
	public boolean failed() {
		return conn.failed();
	}

	@Override
	public Endpoint remoteEndpoint() {
		return conn.remoteEndpoint();
	}

}