package sys.dht.impl;

import static sys.net.api.Networking.Networking;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import sys.RpcServices;
import sys.dht.api.DHT;
import sys.dht.impl.msgs.DHT_Request;
import sys.dht.impl.msgs.DHT_RequestReply;
import sys.dht.impl.msgs.DHT_ResolveKey;
import sys.dht.impl.msgs.DHT_ResolveKeyReply;
import sys.dht.impl.msgs.DHT_StubHandler;
import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcEndpoint;
import sys.utils.Threading;

public class DHT_ClientStub implements DHT {
	private static final int RETRIES = 5;
	private static final int TIMEOUT = 5000;
	
	Endpoint dhtEndpoint;
	RpcEndpoint myEndpoint;

	@Override
	public Endpoint localEndpoint() {
		return myEndpoint.localEndpoint();
	}

	public DHT_ClientStub(final Endpoint dhtEndpoint) {
		this.dhtEndpoint = dhtEndpoint;
		myEndpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toService(RpcServices.DHT.ordinal());
	}

	DHT_ClientStub(final RpcEndpoint myEndpoint, final Endpoint dhtEndpoint) {
		this.myEndpoint = myEndpoint;
		this.dhtEndpoint = dhtEndpoint;
	}

	@Override
	public void send(final Key key, final DHT.Message msg) {
		this.send( new DHT_Request(key, msg) );
	}

	@Override
	public void send(final Key key, final DHT.Message msg, final DHT.ReplyHandler handler) {
		DHT_RequestReply reply = this.send( new DHT_Request( key, msg, true) ) ;
		if( reply != null )
			if( reply.payload != null)
				reply.payload.deliverTo(null, handler);
		else
			handler.onFailure();
	}


	@Override
	public Endpoint resolveKey(final Key key, int timeout) {
		final AtomicReference<Endpoint> ref = new AtomicReference<Endpoint>();
		myEndpoint.send(dhtEndpoint, new DHT_ResolveKey(key), new DHT_StubHandler() {
			public void onReceive(final RpcHandle conn, final DHT_ResolveKeyReply reply) {
				if (key.equals(reply.key))
					ref.set(reply.endpoint);
			}
		}, timeout);
		return ref.get();
	}

	public DHT_RequestReply send( DHT_Request req) {
		final AtomicBoolean done = new AtomicBoolean(false);
		final AtomicReference<DHT_RequestReply> ref = new AtomicReference<DHT_RequestReply>(null);
		for( int i = 0; i < RETRIES ; i++ ) {
			myEndpoint.send(dhtEndpoint, req, new DHT_StubHandler() {
				public void onReceive(final RpcHandle conn, final DHT_RequestReply reply) {
					ref.set( reply );
					done.set(true);
				}
			}, TIMEOUT);
			if( ! done.get() )
				Threading.sleep( 500 * (1+i)) ;
			else break;
		}
		return ref.get();
	}

}
