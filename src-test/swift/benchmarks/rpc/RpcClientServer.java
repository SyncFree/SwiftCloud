package swift.benchmarks.rpc;

import sys.Sys;
import sys.net.api.Networking;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcEndpoint;

public class RpcClientServer extends Handler {

    final static int PORT = 9999;

    RpcEndpoint endpoint;

    RpcClientServer(RpcEndpoint e) {
        endpoint = e;
        endpoint.setHandler(this);
        System.out.println("Server ready...");
    }

    @Override
    public void onReceive(final RpcConnection conn, final Request req) {
        conn.reply(new Reply( req.val ));
    }

    /*
     * The server class...
     */
    public static void main(final String[] args) {

        Sys.init();

        KryoSerialization.init();

        new RpcClientServer(Networking.Networking.rpcBind(PORT, null));
        
        RpcClient.main(null);
    }
}
