package sys.dht.impl;

import static sys.Sys.Sys;
import static sys.dht.catadupa.Config.Config;
import static sys.utils.Log.Log;
import sys.RpcServices;
import sys.dht.DHT_Node;
import sys.dht.api.DHT;
import sys.dht.api.DHT.Connection;
import sys.dht.catadupa.CatadupaNode;
import sys.dht.catadupa.Node;
import sys.dht.discovery.Discovery;
import sys.dht.impl.msgs.DHT_ReplyReply;
import sys.dht.impl.msgs.DHT_Request;
import sys.dht.impl.msgs.DHT_RequestReply;
import sys.dht.impl.msgs.DHT_ResolveKey;
import sys.dht.impl.msgs.DHT_ResolveKeyReply;
import sys.dht.impl.msgs.DHT_StubHandler;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcEndpoint;
import sys.pubsub.impl.PubSubService;
import sys.utils.Threading;

public class DHT_NodeImpl extends CatadupaNode {

	protected static DHT_ClientStub clientStub;
	protected static _DHT_ServerStub serverStub;

	protected DHT_NodeImpl() {
	}

	@Override
	public void init() {
		super.init();

		while (!super.isReady())
			Threading.sleep(50);

		serverStub = new _DHT_ServerStub(new DHT.MessageHandler() {

			@Override
			public void onFailure() {
				Thread.dumpStack();
			}

			@Override
			public void onReceive(Connection conn, DHT.Key key, DHT.Message m) {
				Log.finest(String.format("Un-handled DHT message [<%s,%s>]", key, m.getClass()));
			}
		});

		String name = DHT_Node.DHT_ENDPOINT + Sys.getDatacenter();
		Discovery.register(name, serverStub.getEndpoint().localEndpoint());

		clientStub = new _DHT_ClientStub(serverStub.getEndpoint());

		new PubSubService(rpcFactory);
	}

	@Override
	public void onNodeAdded(Node n) {
		Log.finest("Catadupa added:" + n);
	}

	@Override
	public void onNodeRemoved(Node n) {
		Log.finest("Catadupa removed:" + n);
	}

	protected Node resolveNextHop(final DHT.Key key) {
		long key2key = key.longHashValue() % (1L << Config.NODE_KEY_LENGTH);
		Log.finest(String.format("Hashing %s (%s) @ %s DB:%s", key, key2key, self.key, db.nodeKeys()));
		for (Node i : super.db.nodes(key2key))
			if (i.isOnline())
				return i;

		return self;
	}

	protected class _DHT_ClientStub extends DHT_ClientStub {

		_DHT_ClientStub(RpcEndpoint myEndpoint) {
			super(myEndpoint, myEndpoint.localEndpoint());
		}

		@Override
		public void send(DHT.Key key, DHT.Message msg) {
			Node nextHop = resolveNextHop(key);
			if (nextHop != null) {
				myEndpoint.send(nextHop.endpoint, new DHT_Request(key, msg));
			} else {
				Thread.dumpStack();
			}
		}

		@Override
		public void send(DHT.Key key, DHT.Message msg, DHT.ReplyHandler handler) {
			Node nextHop = resolveNextHop(key);
			if (nextHop != null) {
				myEndpoint.send(nextHop.endpoint, new DHT_Request(key, msg, new DHT_PendingReply(handler).handlerId, myEndpoint.localEndpoint()));
			} else {
				Thread.dumpStack();
			}
		}
	}

	protected class _DHT_ServerStub extends DHT_StubHandler {

		DHT.MessageHandler myHandler;
		final RpcEndpoint myEndpoint;

		_DHT_ServerStub(DHT.MessageHandler myHandler) {
			this.myHandler = myHandler;
			myEndpoint = rpcFactory.toService(RpcServices.DHT.ordinal(), this);
		}

		RpcEndpoint getEndpoint() {
			return myEndpoint;
		}

		public void setHandler(DHT.MessageHandler handler) {
			myHandler = handler;
		}

		public void onReceive(final RpcHandle conn, final DHT_ResolveKey req) {
			Node nextHop = resolveNextHop(req.key);
			if( nextHop != null )
				conn.reply( new DHT_ResolveKeyReply(req.key, nextHop.endpoint)) ;
		}

		@Override
		public void onReceive(RpcHandle conn, DHT_Request req) {
			Node nextHop = resolveNextHop(req.key);
			if (nextHop != null && nextHop.key != self.key) {
				myEndpoint.send(nextHop.endpoint, req);
			} else {
				req.payload.deliverTo(new DHT_ConnectionImpl(conn, req.handlerId, myEndpoint, req.srcEndpoint), req.key, myHandler);
			}
		}

		@Override
		public void onReceive(RpcHandle conn, DHT_RequestReply reply) {
			DHT_PendingReply prh = DHT_PendingReply.getHandler(reply.handlerId);
			if (prh != null) {
				reply.payload.deliverTo(new DHT_ConnectionImpl(conn, reply.replyHandlerId), prh.handler);
			} else {
				Thread.dumpStack();
			}
		}

		@Override
		public void onReceive(RpcHandle conn, DHT_ReplyReply reply) {
			DHT_PendingReply prh = DHT_PendingReply.getHandler(reply.handlerId);
			if (prh != null) {
				reply.payload.deliverTo(new DHT_ConnectionImpl(conn, reply.replyHandlerId), prh.handler);
			} else {
				Thread.dumpStack();
			}
		}
	}

}
