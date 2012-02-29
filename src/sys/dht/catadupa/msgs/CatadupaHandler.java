package sys.dht.catadupa.msgs;

import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class CatadupaHandler implements RpcHandler {

	public void onReceive( RpcConnection conn, CatadupaCast r) {
		Thread.dumpStack();		
	}

	public void onReceive( RpcConnection conn, CatadupaCastPayload r) {
		Thread.dumpStack();		
	}

	public void onReceive( RpcConnection conn, JoinRequest r) {
		Thread.dumpStack();		
	}

	public void onReceive( RpcConnection conn, DbMergeRequest r) {
		Thread.dumpStack();		
	}

	public void onReceive( RpcConnection sock, DbMergeReply r ) {
		Thread.dumpStack();							
	}

	public void onReceive( JoinRequestAccept m ) {
		Thread.dumpStack();				
	}

	public void onReceive( DbMergeReply r ) {
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

	@Override
	public void onReceive(RpcMessage m) {
		Thread.dumpStack();
	}

	@Override
	public void onReceive(RpcConnection conn, RpcMessage m) {
		Thread.dumpStack();
	}
}
