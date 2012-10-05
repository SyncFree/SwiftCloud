package sys.net.impl.rpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.NetworkingException;
import sys.net.api.TransportConnection;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcFactory;
import sys.net.api.rpc.RpcHandler;
import sys.scheduler.PeriodicTask;
import sys.scheduler.Task;
import sys.utils.LongMap;
import sys.utils.Threading;

import static sys.stats.RpcStats.*;

import static sys.Sys.Sys;
import static sys.net.impl.NetworkingConstants.*;

final public class RpcFactoryImpl implements RpcFactory, MessageHandler {

	private static Logger Log = Logger.getLogger( RpcFactoryImpl.class.getName() );

	Endpoint facEndpoint;
	ConnectionManager conMgr;

	public RpcFactoryImpl() {

		// new PeriodicTask(0.0, 10.0) {
		// public void run() {
		// synchronized (conMgr) {
		// for (final TransportConnection[] i : conMgr.ro_connections.values())
		// {
		// new Task(Sys.rg.nextDouble() * 5) {
		// public void run() {
		// try {
		// if (i.length > 0)
		// i[0].send(new RpcPing(Sys.currentTime()));
		// } catch (Exception x) {
		// }
		// }
		// };
		// }
		// }
		// }
		// };

		initStaleHandlersGC_Task();
	}

	public void setEndpoint(Endpoint local) {
		this.facEndpoint = local;
		this.facEndpoint.setHandler(this);
		this.conMgr = new ConnectionManager(local);
	}

	@Override
	synchronized public RpcEndpoint toService(final int service, final RpcHandler handler) {
		RpcEndpoint res = getHandler((long) service);
		if (res == null)
			res = new RpcPacket(this, service, handler);
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
	public void onAccept(final TransportConnection conn) {
		new Task(0) {
			public void run() {
				conMgr.add(conn);
				Log.info("Accepted connection from:" + conn.remoteEndpoint());
			}
		};
	}

	@Override
	public void onConnect(final TransportConnection conn) {
		new Task(0) {
			public void run() {
				conMgr.add(conn);
				Log.info("Established connection to:" + conn.remoteEndpoint());
			}
		};
	}

	@Override
	public void onFailure(final TransportConnection conn) {
		new Task(0) {
			public void run() {
				conMgr.remove(conn);
				Log.info("Connection failed to:" + conn.remoteEndpoint() + "/ cause:" + conn.causeOfFailure());
			}
		};
	}

	@Override
	public void onClose(final TransportConnection conn) {
		new Task(0) {
			public void run() {
				conMgr.remove(conn);
				Log.info("Connection closed to:" + conn.remoteEndpoint());
			}
		};
	}

	public void onReceive(final TransportConnection conn, final RpcPing ping) {
		conn.send(new RpcPong(ping));
	}

	public void onReceive(final TransportConnection conn, final RpcPong pong) {
		RpcStats.logRpcRTT(conn.remoteEndpoint(), pong.rtt());
	}

	public void onReceive(final TransportConnection conn, final RpcPacket pkt) {
		// double t0 = Sys.timeMillis();
		RpcStats.logReceivedRpcPacket(pkt, conn.remoteEndpoint());

		Log.info("RPC: " + pkt.payload.getClass() + " from: " + conn.remoteEndpoint());
		// System.err.println("RPC: " + pkt.payload.getClass() + " from: " +
		// conn.remoteEndpoint() );

		final RpcPacket handler = getHandler(pkt.handlerId);
		if (handler != null) {

			pkt.fac = this;
			pkt.conn = conn;
			pkt.remote = conn.remoteEndpoint();
			handler.deliver(pkt);

			// RpcStats.logRpcExecTime(pkt.payload.getClass(), Sys.timeMillis()
			// - t0);

		} else
			Log.warning("No handler for:" + pkt.payload.getClass() + " " + pkt.handlerId);

	}

	@Override
	public void onReceive(TransportConnection conn, Message m) {
		System.err.println(m.getClass());
		throw new NetworkingException("Incoming object is not an RpcPacket???");
	}

	@Override
	public void onFailure(Endpoint dst, Message m) {
		Thread.dumpStack();
	}

	// synchronized int _g_serial(Object o) {
	// AtomicInteger x = g_serial.get(o);
	// if (x == null)
	// g_serial.put(o, x = new AtomicInteger(0));
	//
	// return x.getAndIncrement();
	// }
	// Map<Object, AtomicInteger> g_serial = new HashMap<Object,
	// AtomicInteger>();

	RpcPacket getHandler(Long hid) {

		if (hid < RPC_MAX_SERVICE_ID) {
			synchronized (handlers0) {
				return handlers0.get(hid);
			}
		} else {
			RpcPacket res;
			synchronized (handlers1) {
				res = handlers1.remove(hid);
			}
			if (res == null)
				return handlers0.get(hid);
			else if (res.deferredRepliesEnabled)
				handlers0.put(hid, res);

			return res;
		}
	}

	final LongMap<RpcPacket> handlers1 = new LongMap<RpcPacket>();
	final ConcurrentHashMap<Long, RpcPacket> handlers0 = new ConcurrentHashMap<Long, RpcPacket>();

	void initStaleHandlersGC_Task() {
		new PeriodicTask(0.0, RPC_GC_STALE_HANDLERS_PERIOD / (1000 * RPC_GC_STALE_HANDLERS_SWEEP_FREQUENCY)) {
			public void run() {
				double now = Sys.timeMillis();
				synchronized (handlers0) {
					for (Iterator<RpcPacket> it = handlers0.values().iterator(); it.hasNext();) {
						RpcPacket p = it.next();
						if (p.timestamp > 0 && (now - p.timestamp) > RPC_GC_STALE_HANDLERS_PERIOD)
							it.remove();
					}
				}
				synchronized (handlers1) {
					List<Long> expired = new ArrayList<Long>();
					for (Iterator<RpcPacket> it = handlers1.values().iterator(); it.hasNext();) {
						RpcPacket p = it.next();
						if (p.timestamp > 0 && (now - p.timestamp) > RPC_GC_STALE_HANDLERS_PERIOD)
							expired.add(p.handlerId);
					}
					for (Long i : expired)
						handlers1.remove(i);
				}
			}
		};
	}
}

final class ConnectionManager {

	private static Logger Log = Logger.getLogger("sys.rpc");

	final Endpoint localEndpoint;
	Map<Endpoint, Set<TransportConnection>> connections = new HashMap<Endpoint, Set<TransportConnection>>();
	Map<Endpoint, TransportConnection[]> ro_connections = Collections.synchronizedMap(new HashMap<Endpoint, TransportConnection[]>());

	ConnectionManager(Endpoint localEndpoint) {
		this.localEndpoint = localEndpoint;
	}

	boolean send(Endpoint remote, RpcPacket pkt) {

		for (int j = 0; j < RPC_CONNECTION_RETRIES; j++) {
			for (TransportConnection i : connections(remote))
				if (sendPacket(i, pkt))
					return true;
			Threading.sleep((j + 1) * RPC_CONNECTION_RETRY_DELAY);
		}
		return false;
	}

	void add(TransportConnection conn) {
		if (!conn.failed()) {
			Set<TransportConnection> cs;
			Endpoint remote = conn.remoteEndpoint();
			synchronized (this) {
				cs = connections.get(remote);
				if (cs == null)
					connections.put(remote, cs = new HashSet<TransportConnection>());
			}
			synchronized (cs) {
				cs.add(conn);
				ro_connections.put(remote, cs.toArray(new TransportConnection[cs.size()]));
				Log.finest("Updated connections to:" + conn.remoteEndpoint() + " : " + cs);
			}
		}
	}

	void remove(TransportConnection conn) {
		Endpoint remote = conn.remoteEndpoint();
		Set<TransportConnection> cs;
		synchronized (this) {
			cs = connections.get(remote);
		}
		if (cs != null) {
			synchronized (cs) {
				cs.remove(conn);
				if (cs.isEmpty())
					ro_connections.remove(remote);
				else
					ro_connections.put(remote, cs.toArray(new TransportConnection[cs.size()]));
				Log.finest("Removed connection to:" + conn.remoteEndpoint() + " : " + cs);
			}
		}
	}

	TransportConnection[] connections(Endpoint remote) {
		TransportConnection[] res = ro_connections.get(remote);
		if (res != null && res.length > 0)
			return res;

		Set<TransportConnection> cs;
		synchronized (this) {
			cs = connections.get(remote);
			if (cs == null)
				connections.put(remote, cs = new HashSet<TransportConnection>());
		}
		synchronized (cs) {
			if (cs.isEmpty()) {
				localEndpoint.connect(remote);
			}
			return noConnections;
		}
	}

	final TransportConnection[] noConnections = new TransportConnection[0];

	// boolean sendX(Endpoint remote, Message m) {
	// for (TransportConnection i : connections(remote))
	// if (i.send(m))
	// return true;
	// return false;
	// }

	/**
	 * Note: Send needs to be done before logging, so the size after
	 * serialization is known...
	 */
	final static boolean sendPacket(TransportConnection conn, RpcPacket pkt) {
		try {
			return conn.send(pkt);
		} finally {
			RpcStats.logSentRpcPacket(pkt, conn.remoteEndpoint());
		}
	}
}