package sys.benchmarks.rpc;

import sys.Sys;
import sys.net.api.Networking;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;

public class RpcServer extends Handler {

    final static int PORT = 9999;

    RpcEndpoint endpoint;

    RpcServer() {
        endpoint = Networking.Networking.rpcBind(PORT, this);
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
        KryoSerialization.init();

        new RpcServer();
    }
}
