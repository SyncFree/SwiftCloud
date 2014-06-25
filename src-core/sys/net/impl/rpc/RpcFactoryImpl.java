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
import static sys.net.impl.NetworkingConstants.RPC_MAX_SERVICE_ID;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
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
import sys.utils.Threading;

final public class RpcFactoryImpl implements RpcFactory, MessageHandler {

    private static Logger Log = Logger.getLogger(RpcFactoryImpl.class.getName());

    Endpoint facEndpoint;
    ConnectionManager conMgr;

    Executor executor = null;

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
        RpcPacket res = getHandler((long) service);
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
        conMgr.add(conn);
    }

    @Override
    public void onConnect(final TransportConnection conn) {
        conMgr.add(conn);
    }

    @Override
    public void onFailure(final TransportConnection conn) {
        conMgr.remove(conn);
    }

    @Override
    public void onClose(final TransportConnection conn) {
        conMgr.remove(conn);
    }

    @Override
    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    static boolean isServer = Sys.mainClass.indexOf("Server") >= 0;
    static double T0 = Sys.currentTime();
    static AtomicInteger totRPCs = new AtomicInteger(0);
    static {
        new PeriodicTask(0, 5) {
            public void run() {
                if (isServer) {
                    double elapsed = Sys.currentTime() - T0;
                    Log.info(String.format("%s RPC/s:%.1f\n", Sys.mainClass, totRPCs.get() / elapsed));
                    if (elapsed > 10.0) {
                        T0 = Sys.currentTime();
                        totRPCs.set(0);
                    }
                }
            }
        };
    }

    public void dispatch(final TransportConnection conn, final RpcPacket pkt) {

        if (executor == null)
            this.onReceive(conn, pkt);
        else {
            pkt.conn = conn;
            this.executor.execute(new Runnable() {
                public void run() {
                    onReceive(pkt.conn, pkt);
                }
            });
        }
    }

    public void onReceive(final TransportConnection conn, final RpcPacket pkt) {
        totRPCs.incrementAndGet();
        double t0 = Sys.timeMillis();

        final RpcPacket handler = getHandler(pkt.handlerId);
        if (handler != null) {
            pkt.fac = this;
            pkt.conn = conn;
            pkt.remote = conn.remoteEndpoint();
            handler.deliver(pkt);

            // System.out.printf("%s exec for: %s, took: %s\n",
            // sys.Sys.Sys.mainClass, pkt.payload.getClass(), Sys.timeMillis() -
            // t0);
            // RpcStats.logRpcExecTime(pkt.payload.getClass(), Sys.timeMillis()
            // - t0);

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

    RpcPacket getHandler(Long hid) {
        RpcPacket res;

        if (hid < RPC_MAX_SERVICE_ID) {
            res = handlers.get(hid);
            if (res != null) {
                res.timestamp = Sys.timeMillis();
                return res;
            }
        }
        res = handlers.remove(hid);
        if (res != null)
            res.timestamp = Sys.timeMillis();
        return res;
    }

    final ConcurrentHashMap<Long, RpcPacket> handlers = new ConcurrentHashMap<Long, RpcPacket>();

    void initStaleHandlersGC_Task() {
        // final Logger Log = Logger.getLogger(RpcFactoryImpl.class.getName() +
        // ".gc");
        //
        // new PeriodicTask(0.0, RPC_GC_STALE_HANDLERS_PERIOD) {
        // public void run() {
        // double now = Sys.timeMillis();
        // synchronized (handlers0) {
        // for (Iterator<RpcPacket> it = handlers0.values().iterator();
        // it.hasNext();) {
        // RpcPacket p = it.next();
        // if (p.handlerId > RPC_MAX_SERVICE_ID && (now - p.timestamp) >
        // p.deferredRepliesTimeout) {
        // it.remove();
        // }
        // }
        // Log.finest("initStaleHandlersGC_Task(): DeferredReplies Handlers: " +
        // handlers0.size());
        // }
        // synchronized (handlers1) {
        // List<Long> expired = new ArrayList<Long>();
        // for (Iterator<RpcPacket> it = handlers1.values().iterator();
        // it.hasNext();) {
        // RpcPacket p = it.next();
        //
        // if (p.timestamp > 0 && (now - p.timestamp) >
        // RPC_GC_STALE_HANDLERS_TIMEOUT)
        // expired.add(p.handlerId);
        // }
        // for (Long i : expired) {
        // handlers1.remove(i);
        // Log.finest("GC'ing Handlers: " + i);
        //
        // }
        // Log.finest("initStaleHandlersGC_Task(): Reply Handlers: " +
        // handlers1.size());
        // }
        // }
        // };
    }
}

final class ConnectionManager {

    private final Logger Log = Logger.getLogger(ConnectionManager.class.toString());

    final Endpoint localEndpoint;

    ConnectionManager(Endpoint localEndpoint) {
        this.localEndpoint = localEndpoint;
    }

    boolean send(Endpoint remote, Message m) {
        for (int j = 0; j < RPC_CONNECTION_RETRIES; j++) {
            int channels = 0;
            for (TransportConnection i : channels(remote)) {
                channels++;
                if (i.send(m))
                    return true;
            }
            if (channels == 0) {
                localEndpoint.connect(remote);
                Threading.sleep((j + 1) * RPC_CONNECTION_RETRY_DELAY);
            }
        }
        return false;
    }

    void add(TransportConnection channel) {
        channels(channel.remoteEndpoint()).add(channel);
        Log.info("Added connection to: " + channel.remoteEndpoint());
    }

    void remove(TransportConnection channel) {
        while (channels(channel.remoteEndpoint()).remove(channel))
            ;
        Log.info("Removed connection to: " + channel.remoteEndpoint());
    }

    CopyOnWriteArrayList<TransportConnection> channels(Endpoint remote) {
        CopyOnWriteArrayList<TransportConnection> cs = connections.get(remote), ncs;
        if (cs == null) {
            cs = connections.putIfAbsent(remote, ncs = new CopyOnWriteArrayList<TransportConnection>());
            if (cs == null)
                cs = ncs;
        }
        return cs;
    }

    final ConcurrentHashMap<Endpoint, CopyOnWriteArrayList<TransportConnection>> connections = new ConcurrentHashMap<Endpoint, CopyOnWriteArrayList<TransportConnection>>();
}