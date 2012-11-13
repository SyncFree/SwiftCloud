package swift.application.swiftdoc.cs.msgs;

import java.util.List;

import swift.application.swiftdoc.TextLine;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class ServerReply implements RpcMessage {

    public List<TextLine> atoms;
    
	ServerReply(){}
	
	public ServerReply( List<TextLine> atoms ){
	    this.atoms = atoms;
	}
	
	@Override
	public void deliverTo(RpcHandle handle, RpcHandler handler) {
		((AppRpcHandler)handler).onReceive(this);			
	}

}
