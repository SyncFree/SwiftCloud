package swift.application.swiftset.cs.msgs;

import swift.application.swiftset.TextLine;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class RemoveAtom extends SwiftSetRpc {

	public TextLine atom;
	
	RemoveAtom(){}
	
	public RemoveAtom( TextLine atom ){
	    this.atom = atom;
	}
	
	@Override
	public void deliverTo(RpcHandle handle, RpcHandler handler) {
		((AppRpcHandler)handler).onReceive(handle, this);
	}

}
