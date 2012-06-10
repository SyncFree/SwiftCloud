package sys.net.impl.rpc;

import static sys.Sys.Sys;
import static sys.utils.Log.Log;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import com.esotericsoftware.kryo.serialize.SimpleSerializer;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.NetworkingException;
import sys.net.api.TransportConnection;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcFactory;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import sys.net.impl.KryoLib;
import sys.scheduler.PeriodicTask;
import sys.utils.LongMap;
import sys.utils.Threading;

import sys.utils.Log;

import static sys.net.impl.KryoLib.*;

final public class RpcFactoryImpl implements RpcFactory, MessageHandler {
	static final long MAX_SERVICE_ID = 1L << 16;
	static final int RPC_MAX_TIMEOUT = 10000;

	Endpoint local;
	ConnectionManager conMgr;
	List<RpcEndpoint> services = new ArrayList<RpcEndpoint>();

	public RpcFactoryImpl() {
		this.conMgr = new ConnectionManager();

//		sys.utils.Log.setLevel("", Level.ALL);
//		sys.utils.Log.setLevel("sys.dht.catadupa", Level.ALL);
//		sys.utils.Log.setLevel("sys.dht", Level.FINE);
//		sys.utils.Log.setLevel("sys.net", Level.ALL);
//		sys.utils.Log.setLevel("sys", Level.ALL);

		KryoLib.register(RpcPacket.class, new SimpleSerializer<RpcPacket>() {

			public RpcPacket read(ByteBuffer bb) {
				RpcPacket res = new RpcPacket();
				res.handlerId = bb.getLong();
				res.replyHandlerId = bb.getLong();
				res.payload = (RpcMessage) kryo.readClassAndObject(bb);
				return res;
			}

			@Override
			public void write(ByteBuffer bb, RpcPacket pkt) {
				bb.putLong(pkt.handlerId);
				bb.putLong(pkt.replyHandlerId);
				kryo.writeClassAndObject(bb, pkt.payload);
			}
		});

		gcStaleHandlers();
	}

	public void setEndpoint(Endpoint local) {
		this.local = local;
		this.local.setHandler(this);
	}

	@Override
	public RpcEndpoint toService(final int service, final RpcHandler handler) {
		RpcEndpoint res = new RpcPacket(service, handler);
		services.add(res);
		return res;
	}

	@Override
	public RpcEndpoint toService(int service) {
		return toService(service, null);
	}

	@Override
	public RpcEndpoint toDefaultService() {
		return toService(0);
	}

	@Override
	public void onAccept(TransportConnection conn) {
		conMgr.add(conn);
		Log.finest("Accepted connection from:" + conn.remoteEndpoint());
	}

	@Override
	public void onConnect(TransportConnection conn) {
		Log.finest("Established connection to:" + conn.remoteEndpoint());
		conMgr.add(conn);
	}

	@Override
	public void onFailure(TransportConnection conn) {
		Log.finest("Connection failed to:" + conn.remoteEndpoint() + "/ cause:" + conn.causeOfFailure());
		conMgr.remove(conn);
	}

	@Override
	public void onClose(TransportConnection conn) {
		Log.finest("Connection closed to:" + conn.remoteEndpoint());
		conMgr.remove(conn);
	}

	public static AtomicInteger rpcCounter = new AtomicInteger();
	static {
		new PeriodicTask( 0.0, 5.0 ) {
			public void run() {
				int current = rpcCounter.get();
				if( current > 0 )
					Log.finest(String.format("Total RPCs: %.2f K\n", current / 1000.0));
			}
		};
	}
	
	public void onReceive(final TransportConnection conn, final RpcPacket pkt) {
		rpcCounter.incrementAndGet();
		
		Log.finest("RPC: " + pkt.payload.getClass() );
		
		final RpcPacket handle = getHandle(pkt);
		if (handle != null) {
			pkt.conn = conn;
			pkt.remote = conn.remoteEndpoint();
			handle.accept(pkt);
		} else {
			Log.finest("No handler for:" + pkt.payload.getClass() + " " + pkt.handlerId);
			System.err.println("Ooops. No handler for:" + pkt.payload.getClass() + " " + pkt.handlerId);
		}
	}

	@Override
	public void onReceive(TransportConnection conn, Message m) {
		throw new NetworkingException("Incoming object is not an RpcPacket???");
	}

	@Override
	public void onFailure(Endpoint dst, Message m) {
		Thread.dumpStack();
	}

	final class ConnectionManager {
		final int CONNECTION_RETRIES = 3;
		Map<Endpoint, TransportConnection[]> ro_connections = new HashMap<Endpoint, TransportConnection[]>();
		Map<Endpoint, Set<TransportConnection>> connections = new HashMap<Endpoint, Set<TransportConnection>>();

		boolean send(Endpoint remote, Message msg) {
			for (TransportConnection i : connections(remote))
				if (i.send(msg))
					return true;

			for (int j = 0; j < CONNECTION_RETRIES; j++) {
				TransportConnection res = local.connect(remote);
				if (res != null && res.send(msg))
					return true;
				Threading.sleep(100);
			}
			return false;
		}

		synchronized void add(TransportConnection conn) {
			if (!conn.failed()) {
				Endpoint remote = conn.remoteEndpoint();
				Set<TransportConnection> cs = connections.get(remote);
				if( cs == null ) {
					connections.put(remote, cs = new HashSet<TransportConnection>() );
					cs.add( conn ) ;
				}
				ro_connections.put( remote, cs.toArray( new TransportConnection[ cs.size() ] ) ) ;
			}
		}

		synchronized void remove(TransportConnection conn) {
			Endpoint remote = conn.remoteEndpoint();
			Set<TransportConnection> cs = connections.get(remote);
			if( cs != null ) {
				cs.remove( conn ) ;
				if( cs.isEmpty() )
					ro_connections.remove( remote ) ;
				else
					ro_connections.put( remote, cs.toArray( new TransportConnection[ cs.size() ] ) ) ;
			}			
		}

		synchronized TransportConnection[] connections(Endpoint remote) {
			TransportConnection[] res = ro_connections.get(remote);
			return res != null ? res : noConnections;
		}
		
		final TransportConnection[] noConnections = new TransportConnection[0];
	}

	final public class RpcPacket extends AbstractRpcPacket {

		boolean isWaiting4Reply = false;

		RpcPacket() {
		}

		RpcPacket(long service, RpcHandler handler) {
			this.timeout = -1;
			this.handler = handler;
			this.handlerId = service;
			this.replyHandlerId = service;
			handles.put(this.handlerId, new StaleRef(this));
		}

		private RpcPacket(Endpoint remote, RpcMessage payload, RpcPacket handle, RpcHandler replyhandler, int timeout) {
			this.remote = remote;
			this.payload = payload;
			this.handler = replyhandler;
			this.handlerId = handle.replyHandlerId;
			this.timeout = Math.min(timeout, RPC_MAX_TIMEOUT);
			if (replyhandler != null) {
				synchronized (handles) {
					this.timestamp = Sys.timeMillis();
					this.replyHandlerId = g_handlers++;
					handles.put(this.replyHandlerId, new StaleRef(this));
				}
			} else
				this.replyHandlerId = 0L;
		}

		@Override
		public Endpoint localEndpoint() {
			return local;
		}

		@Override
		public RpcHandle send(Endpoint remote, RpcMessage msg, RpcHandler replyHandler, int timeout) {
			RpcPacket pkt = new RpcPacket(remote, msg, this, replyHandler, timeout);

			if (timeout != 0)
				synchronized (pkt) {
					// System.out.println("sync for:" + pkt.hashCode() );
					pkt.isWaiting4Reply = true;
					if (pkt.sendRpcSuccess(null, this))
						pkt.waitForReply();
				}
			else {
				pkt.remote = remote;
				pkt.sendRpcSuccess(null, this);
			}
			return pkt;
		}

		public RpcHandle reply(RpcMessage msg, RpcHandler replyHandler, int timeout) {
			RpcPacket pkt = new RpcPacket(remote, msg, this, replyHandler, timeout);

			if (timeout != 0)
				synchronized (pkt) {
					// System.out.println("sync for:" + pkt.hashCode() );
					pkt.isWaiting4Reply = true;
					if (pkt.sendRpcSuccess(conn, this))
						pkt.waitForReply();
				}
			else
				pkt.sendRpcSuccess(conn, this);
			return pkt;
		}

		final void accept(RpcPacket pkt) {
			if (isWaiting4Reply) {
				synchronized (this) {
					reply = pkt;
					Threading.notifyOn(this);
				}
			} else
				pkt.payload.deliverTo(pkt, this.handler);
		}

		final private void waitForReply() {
			while (reply == null && !timedOut());

			isWaiting4Reply = false;
			if (reply != null)
				reply.payload.deliverTo(reply, this.handler);
		}

		final private boolean timedOut() {
			int ms = (int)( (timeout < 0 ? RPC_MAX_TIMEOUT : timeout) - (Sys.timeMillis() - timestamp));
			if (ms > 0)
				Threading.waitOn(this, ms > 100 ? 100 : ms);
			return ms <= 0;
		}

		final private boolean sendRpcSuccess(TransportConnection conn, AbstractRpcPacket handle) {
			try {
				if (conn != null && conn.send(this) || conMgr.send(remote, this)) {
					payload = null;
					return true;
				} else {
					synchronized (handles) {
						handles.remove(this.replyHandlerId);
					}
					if (handler != null)
						handler.onFailure(this);
					else if (handle.handler != null)
						handle.handler.onFailure(this);

					return false;
				}
			} catch (Throwable t) {
				t.printStackTrace();
				failed = true;
				failureCause = t;

				if (handler != null)
					handler.onFailure(this);
				else
					handle.handler.onFailure(this);

				return false;
			}
		}

		public String toString() {
			return String.format("RPC(%s,%s,%s)", handlerId, replyHandlerId, this.handler);
		}

		@Override
		public void deliverTo(TransportConnection conn, MessageHandler handler) {
			((RpcFactoryImpl) handler).onReceive(conn, this);
		}

		
		@Override
		public RpcHandle enableStreamingReplies(boolean flag) {
			streamingIsEnabled = flag;
			if( streamingIsEnabled )
				synchronized (handles) {
					handles.put(replyHandlerId, new StaleRef(this) );
				}
			return this;
		}
	}

	RpcPacket getHandle(RpcPacket other) {
		synchronized (handles) {
			
			if (other.handlerId < MAX_SERVICE_ID) {
				StaleRef ref = handles.get(other.handlerId);
				return ref == null ? null : ref.get();				
			}
			else {
				RpcPacket res = null;
				StaleRef ref = handles.get(other.handlerId);
				if( ref != null && (res = ref.get()).streamingIsEnabled )
					handles.put(res.replyHandlerId, ref);
				
				return res;
			}
		}
	}

	void gcStaleHandlers() {
		Threading.newThread(true, new Runnable() {

			@Override
			public void run() {
				for (;;) {
					try {
						StaleRef ref = (StaleRef) refQueue.remove();
						synchronized (handles) {
							handles.remove(ref.key);
							while ((ref = (StaleRef) refQueue.poll()) != null)
								handles.remove(ref.key);
						}
					} catch (Throwable t) {
						t.printStackTrace();
					}
				}
			}

		}).start();
	}

	final class StaleRef extends SoftReference<RpcPacket> {

		final long key;

		public StaleRef(RpcPacket referent) {
			super(referent, refQueue);
			this.key = referent.replyHandlerId;
		}

	}

	// [0-MAX_SERVICE_ID[ are reserved for static service handlers.
	long g_handlers = MAX_SERVICE_ID + Sys.rg.nextInt(100000);

	final LongMap<StaleRef> handles = new LongMap<StaleRef>();
	final ReferenceQueue<RpcPacket> refQueue = new ReferenceQueue<RpcPacket>();

}
