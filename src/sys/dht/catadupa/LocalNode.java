package sys.dht.catadupa;

import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcEndpoint;
import sys.dht.catadupa.msgs.CatadupaHandler;

import static sys.net.api.Networking.Networking;

/**
 * 
 * @author smd
 *
 */
public class LocalNode extends CatadupaHandler {

	protected Node self;
	protected RpcEndpoint rpc ;
	protected Endpoint endpoint;

	LocalNode() {
		initLocalNode();
	}
	
	public void initLocalNode() { 
		KryoSerialization.init();
		rpc = Networking.rpcBind(0, this) ;
		self = new Node( rpc.localEndpoint() ) ;		
		SeedDB.init(self);
	}
}
