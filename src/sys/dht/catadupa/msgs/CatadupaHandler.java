package sys.dht.catadupa.msgs;

import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class CatadupaHandler implements RpcHandler {

	public void onReceive( RpcHandle conn, CatadupaCast r) {
		Thread.dumpStack();		
	}

	public void onReceive( RpcHandle conn, CatadupaCastPayload r) {
		Thread.dumpStack();		
	}

	public void onReceive( RpcHandle conn, JoinRequest r) {
		Thread.dumpStack();		
	}

	public void onReceive( RpcHandle conn, DbMergeRequest r) {
		Thread.dumpStack();		
	}

	public void onReceive( RpcHandle sock, DbMergeReply r ) {
		Thread.dumpStack();							
	}

	public void onReceive( JoinRequestAccept m ) {
		Thread.dumpStack();				
	}

	public void onReceive( DbMergeReply r ) {
		Thread.dumpStack();							
	}

	@Override
	public void onReceive(RpcMessage m) {
		Thread.dumpStack();
	}

	@Override
	public void onFailure( RpcHandle handle) {
		Thread.dumpStack();
	}

	@Override
	public void onReceive(RpcHandle conn, RpcMessage m) {
		Thread.dumpStack();
	}
}
