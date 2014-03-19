/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package sys.net.impl.rpc;

import static sys.Sys.Sys;
import static sys.net.impl.NetworkingConstants.RPC_CONNECTION_RETRIES;
import static sys.net.impl.NetworkingConstants.RPC_CONNECTION_RETRY_DELAY;
import static sys.net.impl.NetworkingConstants.RPC_GC_STALE_HANDLERS_PERIOD;
import static sys.net.impl.NetworkingConstants.RPC_GC_STALE_HANDLERS_TIMEOUT;
import static sys.net.impl.NetworkingConstants.RPC_MAX_SERVICE_ID;
import static sys.stats.RpcStats.RpcStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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

final public class RpcFactoryImpl implements RpcFactory, MessageHandler {

    private static Logger Log = Logger.getLogger(RpcFactoryImpl.class.getName());

    Endpoint facEndpoint;
    ConnectionManager conMgr;

    public RpcFactoryImpl() {
        initStaleHandlersGC_Task();
    }

    public void setEndpoint(Endpoint local) {
        this.facEndpoint = local;
        this.facEndpoint.setHandler(this);
        this.conMgr = new ConnectionManager(local);
    }

    @Override
    synchronized public RpcEndpoint toService(final int service, final RpcHandler handler) {
        RpcPacket res = getHandler((long) service, false);
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

    static boolean isServer = Sys.mainClass.indexOf("Server") >= 0;
    static double T0 = Sys.currentTime();
    static AtomicInteger totRPCs = new AtomicInteger(0);
    static {
        new PeriodicTask(0, 5) {
            public void run() {
                if (isServer) {
                    double elapsed = Sys.currentTime() - T0;
                    System.err.printf("%s RPC/s:%.1f\n", Sys.mainClass, totRPCs.get() / elapsed);
                    if (elapsed > 10.0) {
                        T0 = Sys.currentTime();
                        totRPCs.set(0);
                    }
                }
            }
        };
    }

    public void onReceive(final TransportConnection conn, final RpcPacket pkt) {
        totRPCs.incrementAndGet();
        // if (sys.Sys.Sys.mainClass.contains("Sequencer"))
        // System.out.println(pkt.getPayload().getClass());

        // if( isServer && totRPCs.get() % 999 == 0 )
        // System.err.println(sys.Sys.Sys.mainClass + "/%%%%%%%" + totRPCs.get()
        // );

        double t0 = Sys.timeMillis();

        RpcStats.logReceivedRpcPacket(pkt, conn.remoteEndpoint());

        // if( isServer)
        // Log.info("RPC: " + pkt.payload.getClass() + " from: " +
        // conn.remoteEndpoint());
        // System.err.println(IP.localHostAddressString() + " RPC: " +
        // pkt.payload.getClass() + " from: " + conn.remoteEndpoint() );

        final RpcPacket handler = getHandler(pkt.handlerId, pkt.deferredRepliesTimeout > 0);
        if (handler != null) {
            pkt.fac = this;
            pkt.conn = conn;
            pkt.remote = conn.remoteEndpoint();
            handler.deliver(pkt);

            // System.out.printf("%s exec for: %s, took: %s\n",
            // sys.Sys.Sys.mainClass, pkt.payload.getClass(), Sys.timeMillis() -
            // t0);
            RpcStats.logRpcExecTime(pkt.payload.getClass(), Sys.timeMillis() - t0);

        } else
            Log.warning(sys.Sys.Sys.mainClass + " - No handler for:" + pkt.payload.getClass() + " " + pkt.handlerId);
    }

    @Override
    public void onReceive(TransportConnection conn, Message m) {
        throw new NetworkingException("Incoming object is not an RpcPacket???");
    }

    @Override
    public void onFailure(Endpoint dst, Message m) {
        Thread.dumpStack();
    }

    RpcPacket getHandler(Long hid, boolean deferredRepliesEnabled) {
        RpcPacket res;

        if (hid < RPC_MAX_SERVICE_ID || deferredRepliesEnabled) {
            res = handlers0.get(hid);
            if (res != null) {
                res.timestamp = Sys.timeMillis();
                return res;
            }
        }

        synchronized (handlers1) {
            res = handlers1.remove(hid);
            if (res != null && deferredRepliesEnabled) {
                handlers0.put(hid, res);
                res.timestamp = Sys.timeMillis();
            }
            return res;
        }
    }

    final LongMap<RpcPacket> handlers1 = new LongMap<RpcPacket>();
    final ConcurrentHashMap<Long, RpcPacket> handlers0 = new ConcurrentHashMap<Long, RpcPacket>();

    void initStaleHandlersGC_Task() {
        final Logger Log = Logger.getLogger(RpcFactoryImpl.class.getName() + ".gc");

        new PeriodicTask(0.0, RPC_GC_STALE_HANDLERS_PERIOD) {
            public void run() {
                double now = Sys.timeMillis();
                synchronized (handlers0) {
                    for (Iterator<RpcPacket> it = handlers0.values().iterator(); it.hasNext();) {
                        RpcPacket p = it.next();
                        if (p.handlerId > RPC_MAX_SERVICE_ID && (now - p.timestamp) > p.deferredRepliesTimeout) {
                            it.remove();
                        }
                    }
                    Log.finest("initStaleHandlersGC_Task(): DeferredReplies Handlers: " + handlers0.size());
                }
                synchronized (handlers1) {
                    List<Long> expired = new ArrayList<Long>();
                    for (Iterator<RpcPacket> it = handlers1.values().iterator(); it.hasNext();) {
                        RpcPacket p = it.next();

                        if (p.timestamp > 0 && (now - p.timestamp) > RPC_GC_STALE_HANDLERS_TIMEOUT)
                            expired.add(p.handlerId);
                    }
                    for (Long i : expired) {
                        handlers1.remove(i);
                        Log.finest("GC'ing Handlers: " + i);

                    }
                    Log.finest("initStaleHandlersGC_Task(): Reply Handlers: " + handlers1.size());
                }
            }
        };
    }
}

final class ConnectionManager {

    private static Logger Log = Logger.getLogger("sys.rpc");

    final Endpoint localEndpoint;
    Map<Endpoint, Set<TransportConnection>> connections = new HashMap<Endpoint, Set<TransportConnection>>();
    Map<Endpoint, TransportConnection[]> ro_connections = Collections
            .synchronizedMap(new HashMap<Endpoint, TransportConnection[]>());

    ConnectionManager(Endpoint localEndpoint) {
        this.localEndpoint = localEndpoint;
    }

    boolean send(Endpoint remote, RpcPacket pkt) {

        for (int j = 0; j < RPC_CONNECTION_RETRIES; j++) {
            for (TransportConnection i : connections(remote))
                if (i.send(pkt))
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
}