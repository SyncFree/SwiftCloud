package swift.application.swiftset.cs.msgs;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class CommitTransaction extends SwiftSetRpc {

	public CommitTransaction(){}
		
	@Override
	public void deliverTo(RpcHandle handle, RpcHandler handler) {
		((AppRpcHandler)handler).onReceive(handle, this);
	}

}
