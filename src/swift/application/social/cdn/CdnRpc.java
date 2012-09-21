package swift.application.social.cdn;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class CdnRpc implements RpcMessage {

	String payload;
	
	CdnRpc(){}
	
	public CdnRpc( String payload ){
		this.payload = payload;
	}
	
	@Override
	public void deliverTo(RpcHandle handle, RpcHandler handler) {
		if( handle.expectingReply() )
			((CdnRpcHandler)handler).onReceive(handle, this);
		else
			((CdnRpcHandler)handler).onReceive(this);			
	}

}
