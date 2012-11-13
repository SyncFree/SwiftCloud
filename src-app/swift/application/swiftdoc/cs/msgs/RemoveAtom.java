package swift.application.swiftdoc.cs.msgs;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class RemoveAtom extends SwiftDocRpc {

	public int pos;
	
	RemoveAtom(){}
	
	public RemoveAtom( int pos ){
	    this.pos = pos;
	}
	
	@Override
	public void deliverTo(RpcHandle handle, RpcHandler handler) {
		((AppRpcHandler)handler).onReceive(handle, this);
	}

}
