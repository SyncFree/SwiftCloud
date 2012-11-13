package sys.net.impl;

import sys.Sys;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;

import static sys.net.api.Networking.*;

public class RpcServer extends Handler {

    final static int PORT = 9999;

    RpcEndpoint endpoint;

    RpcServer() {
        endpoint = Networking.rpcBind(PORT, TransportProvider.DEFAULT).toService(0, this);
        
        System.out.println("Server ready...");
    }
    
    @Override
    public void onReceive(final RpcHandle handle, final Request req) {
//    	System.err.println("Server: " + req );
        handle.reply(new Reply( req.val, req.timestamp ));
    }

    /*
     * The server class...
     */
    public static void main(final String[] args) {

        Sys.init();

        new RpcServer();
    }
}
