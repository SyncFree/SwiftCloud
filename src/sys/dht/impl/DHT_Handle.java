package sys.dht.impl;

import sys.dht.api.*;
import sys.dht.api.DHT.Reply;
import sys.dht.api.DHT.ReplyHandler;
import sys.dht.impl.msgs.DHT_RequestReply;
import sys.net.api.rpc.RpcHandle;

public class DHT_Handle implements DHT.Handle {

	RpcHandle handle;
	boolean expectingReply;
	
	DHT_Handle( RpcHandle handle, boolean expectingReply ) {
		this.handle = handle;
		this.expectingReply = expectingReply;
	}
	
	@Override
	public boolean expectingReply() {
		return expectingReply;
	}

	@Override
	public boolean reply(Reply msg) {
		return handle.reply( new DHT_RequestReply(msg)).succeeded() ;
	}

	@Override
	public boolean reply(Reply msg, ReplyHandler handler) {
		throw new RuntimeException("Not implemented");
	}
}
