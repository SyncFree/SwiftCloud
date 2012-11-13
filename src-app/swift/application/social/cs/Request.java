package swift.application.social.cs;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class Request implements RpcMessage {

	String payload;
	
	Request(){}
	
	public Request( String payload ){
		this.payload = payload;
	}
	
	@Override
	public void deliverTo(RpcHandle handle, RpcHandler handler) {
		if( handle.expectingReply() )
			((RequestHandler)handler).onReceive(handle, this);
		else
			((RequestHandler)handler).onReceive(this);			
	}

}
