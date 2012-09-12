package sys.benchmarks.rpc;



import java.net.UnknownHostException;
import java.util.logging.Level;

import sys.net.impl.KryoLib;
import sys.utils.Log;
import sys.utils.Threading;


public class RpcClientServer {

    public static void main(final String[] args) throws UnknownHostException {
        Log.setLevel("", Level.OFF);
        Log.setLevel("sys.dht.catadupa", Level.OFF);
        Log.setLevel("sys.dht", Level.OFF);
        Log.setLevel("sys.net", Level.OFF);
        Log.setLevel("sys", Level.OFF);

        KryoLib.register(Request.class, 0x100);
        KryoLib.register(Reply.class, 0x101);
        
        RpcServer.main( new String[0] );

        for( int i = 0 ; i < 1 ; i++ )
        	Threading.newThread(true, new Runnable() {
        		public void run() {
        			try {
						RpcClient.main( new String[0]) ;
					} catch (Throwable t) {
						t.printStackTrace();
					}        			
        		}
        	}).start();

    }
}
