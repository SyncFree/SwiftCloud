package swift.application.swiftdoc.cs.msgs;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class BeginTransaction extends SwiftDocRpc {

	public BeginTransaction(){
	}
	
	@Override
	public void deliverTo(RpcHandle handle, RpcHandler handler) {
		((AppRpcHandler)handler).onReceive(handle, this);
	}

}
