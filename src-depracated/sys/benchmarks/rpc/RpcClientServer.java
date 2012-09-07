package sys.benchmarks.rpc;


import static sys.net.impl.KryoLib.kryo;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.logging.Level;

import com.esotericsoftware.kryo.serialize.SimpleSerializer;

import sys.net.api.rpc.RpcMessage;
import sys.net.impl.KryoLib;
import sys.net.impl.rpc.RpcFactoryImpl.RpcPacket;
import sys.utils.Log;
import sys.utils.Threading;


public class RpcClientServer {

    public static void main(final String[] args) throws UnknownHostException {
        Log.setLevel("", Level.OFF);
        Log.setLevel("sys.dht.catadupa", Level.OFF);
        Log.setLevel("sys.dht", Level.OFF);
        Log.setLevel("sys.net", Level.OFF);
        Log.setLevel("sys", Level.OFF);

		KryoLib.register(Request.class, new SimpleSerializer<Request>() {

			@Override
			public Request read(ByteBuffer bb) {
				return new Request( bb.getInt(), bb.getDouble() ) ;
			}

			@Override
			public void write(ByteBuffer bb, Request req) {
				bb.putInt( req.val );
				bb.putDouble( req.timestamp ) ;
			}
		});

		KryoLib.register(Reply.class, new SimpleSerializer<Reply>() {

			@Override
			public Reply read(ByteBuffer bb) {
				return new Reply( bb.getInt(), bb.getDouble() );
			}

			@Override
			public void write(ByteBuffer bb, Reply rep) {
				bb.putInt( rep.val );
				bb.putDouble( rep.timestamp ) ;
			}
		});

        RpcServer.main( new String[0] );

        for( int i = 0 ; i < 10 ; i++ )
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
