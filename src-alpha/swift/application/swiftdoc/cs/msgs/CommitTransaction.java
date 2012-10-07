package swift.application.swiftdoc.cs.msgs;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class CommitTransaction extends SwiftDocRpc {

	public CommitTransaction(){}
		
	@Override
	public void deliverTo(RpcHandle handle, RpcHandler handler) {
		((AppRpcHandler)handler).onReceive(handle, this);
	}

}
