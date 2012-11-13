package swift.application.swiftdoc.cs.msgs;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class InitScoutServer extends SwiftDocRpc {

	public InitScoutServer(){
	}
	
	@Override
	public void deliverTo(RpcHandle handle, RpcHandler handler) {
		((AppRpcHandler)handler).onReceive(handle, this);
	}

}
