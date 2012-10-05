package sys.benchmarks.rpc;



import java.net.UnknownHostException;
import java.util.logging.Logger;

import sys.net.impl.KryoLib;
import sys.utils.Threading;


public class RpcClientServer {
    public static Logger Log = Logger.getLogger( RpcClientServer.class.getName() );

    public static void main(final String[] args) throws UnknownHostException {

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
