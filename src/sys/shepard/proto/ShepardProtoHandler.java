package sys.shepard.proto;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class ShepardProtoHandler implements RpcHandler {

    @Override
    public void onReceive(RpcMessage m) {
    }

    @Override
    public void onReceive(RpcHandle handle, RpcMessage m) {
    }

    @Override
    public void onFailure(RpcHandle handle) {
    }

    public void onReceive( RpcHandle client, GrazingRequest q ) {
    }

    public void onReceive( GrazingGranted p ) {        
    }
    
    public void onReceive( GrazingAccepted p ) {   
        System.err.println("Request Accepted from Shepard!!!");
    }
}
