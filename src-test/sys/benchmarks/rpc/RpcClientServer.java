package sys.benchmarks.rpc;


import java.net.UnknownHostException;
import java.util.logging.Level;

import sys.utils.Log;


public class RpcClientServer {

    public static void main(final String[] args) throws UnknownHostException {
        Log.setLevel("", Level.OFF);
        Log.setLevel("sys.dht.catadupa", Level.OFF);
        Log.setLevel("sys.dht", Level.OFF);
        Log.setLevel("sys.net", Level.OFF);
        Log.setLevel("sys", Level.OFF);

        RpcServer.main( new String[0] );

		RpcClient.main( new String[0]) ;

    }
}
