package sys.dht.catadupa.msgs;

import java.util.Map;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

import sys.dht.catadupa.MembershipUpdate;
import sys.dht.catadupa.crdts.ORSet;
import sys.dht.catadupa.crdts.time.LVV;
import sys.dht.catadupa.crdts.time.Timestamp;

public class DbMergeReply implements RpcMessage {
	
	public LVV clock;
//	public ORSet<MembershipUpdate> delta;
	public Map<MembershipUpdate,Timestamp> delta;
	
	DbMergeReply() {
	}

	public DbMergeReply( LVV clock, ORSet<MembershipUpdate> delta ) {
		this.clock = clock;
//		this.delta = delta.isEmpty() ? null : delta;
	}
	
	public DbMergeReply( LVV clock, Map<MembershipUpdate,Timestamp> delta ) {
		this.clock = clock;
		this.delta = delta ;
	}
	
	public ORSet<MembershipUpdate> toORSet() {
		ORSet<MembershipUpdate> res = new ORSet<MembershipUpdate>();
		for( Map.Entry<MembershipUpdate, Timestamp> i : delta.entrySet() )
			res.add( i.getKey(), i.getValue() );
		return res;
	}
	
	public void deliverTo( RpcConnection conn, RpcHandler handler) {		
		if( conn.expectingReply() )
			((CatadupaHandler) handler).onReceive( conn, this ) ;			
		else
			((CatadupaHandler) handler).onReceive( this ) ;
	}
}
