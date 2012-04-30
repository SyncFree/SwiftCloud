package sys.net.impl.rpc;

import static sys.Sys.Sys;
import static sys.utils.Log.Log;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.NetworkingException;
import sys.net.api.TcpConnection;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcFactory;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import sys.scheduler.PeriodicTask;
import sys.scheduler.Task;
import sys.utils.Threading;

public class RpcFactoryImpl implements RpcFactory, MessageHandler {

    Endpoint endpoint;
    Map<Endpoint, MultiplexedConnection> connections = Collections.synchronizedMap(new HashMap<Endpoint, MultiplexedConnection>());

    public RpcFactoryImpl(Endpoint endpoint) {
        this.endpoint = endpoint;
        this.endpoint.setHandler(this);
        this.gcHandlers();
    }

    @Override
    public RpcEndpoint rpcService(final int service, final RpcHandler handler) {
        return new RpcEndpointImpl(service, handler);
    }

    public void onReceive(final TcpConnection conn, final RpcPacket m) {
        MultiplexedConnection mc = createIncomingMultiplexedConnection(conn);
        receiveRpc(mc, m);
    }

    class MultiplexedConnection {
        final Endpoint remote;
        final TcpConnection connection;
        double lastUse = Sys.currentTime();
        boolean isOutgoing, isIncoming;

        MultiplexedConnection(Endpoint remote, TcpConnection connection) {
            this.remote = remote;
            this.connection = connection;
            lastUse = Sys.currentTime();
            acceptIncomingRpcPackets();
            isIncoming = false;
            isOutgoing = true;
        }

        MultiplexedConnection(TcpConnection connection) {
            remote = connection.remoteEndpoint();
            this.connection = connection;
            lastUse = Sys.currentTime();
            acceptIncomingRpcPackets();
            isIncoming = true;
            isOutgoing = false;
        }

        MultiplexedConnection touch() {
            lastUse = Sys.currentTime();
            return this;
        }

        private void acceptIncomingRpcPackets() {
            new Task(0) {
                @Override
                public void run() {
                    try {
                        for (;;) {
                            RpcPacket m = connection.receive();
                            if (m != null)
                                receiveRpc(MultiplexedConnection.this, m);
                            else
                                break;
                        }
                    } catch (Throwable t) {
                        // t.printStackTrace();
                    }
                    synchronized (connections) {
                        connection.dispose();
                        connections.remove(MultiplexedConnection.this);
                    }
                }
            };
        }
    }

    void receiveRpc(final MultiplexedConnection mc, final RpcPacket pkt) {
        final RPC_Handlers pr = getHandler(pkt.handlerId);
        if (pr != null) {
            if (pr.isSynchronous())
                pr.setReply(pkt);
            else
                new Task(0) {
                    @Override
                    public void run() {
                        invokeRpc(mc, pkt, pr.handler);
                    }
                };
        }
    }

    void invokeRpc(MultiplexedConnection mc, RpcPacket pkt, RpcHandler h) {
        Log.finest("" + pkt.payload.getClass());

        RpcConnectionImpl rpc = new RpcConnectionImpl(mc.connection, pkt.replyHandlerId);
        pkt.payload.deliverTo(rpc, h);
    }

    boolean sendRpc(Endpoint remote, RpcMessage m, long remoteHandlerId, RpcHandler handler, int timeout) {
        try {
            boolean ok = false;
            RpcPacket pkt;
            RPC_Handlers pr = null;

            if (handler != null) {
                pr = new RPC_Handlers(handler, timeout);
                pkt = new RpcPacket(m, remoteHandlerId, handler == null ? 0L : pr.handlerId);
            } else
                pkt = new RpcPacket(m, remoteHandlerId, 0L);

            MultiplexedConnection mc = getOutgoingMultiplexedConnection(remote);
            if (mc != null && !mc.connection.failed())
                ok = mc.connection.send(pkt);
            else if (handler != null)
                handler.onFailure();

            if (ok && pr != null && pr.blockForReply())
                invokeRpc(mc, pr.reply, pr.handler);
            return ok;

        } catch (Throwable t) {
            t.printStackTrace();
        }
        return false;
    }

    public class RpcEndpointImpl implements RpcEndpoint {

        int service;
        RpcHandler handler;
        RpcHandler failureHandler;

        public RpcEndpointImpl(int service, RpcHandler handler) {
            this.handler = handler;
            this.service = service;
            failureHandler = handler;
            new RPC_Handlers(service, handler);
        }

        @Override
        public void setHandler(RpcHandler handler) {
            new RPC_Handlers(service, handler);
        }

        @Override
        public Endpoint localEndpoint() {
            return endpoint;
        }

        @Override
        public boolean send(final Endpoint dst, final RpcMessage m) {
            return sendRpc(dst, m, service, null, 0);
        }

        @Override
        public boolean send(final Endpoint dst, final RpcMessage m, final RpcHandler replyHandler) {
            return sendRpc(dst, m, service, replyHandler, replyHandler == null ? 0 : Integer.MAX_VALUE);
        }

        @Override
        public boolean send(Endpoint dst, RpcMessage m, RpcHandler replyHandler, int timeout) {
            return sendRpc(dst, m, service, replyHandler, replyHandler == null ? 0 : timeout);
        }

        @Override
        public String toString() {
            return endpoint + ":" + service + "->" + handler;
        }
    }

    class RpcConnectionImpl extends AbstractRpcConnection {

        final Endpoint remote;
        final long remoteHandlerId;

        protected RpcConnectionImpl(TcpConnection conn, long remoteHandlerId) {
            super(conn, remoteHandlerId > 1000);
            remote = conn.remoteEndpoint();
            this.remoteHandlerId = remoteHandlerId;
        }

        @Override
        public boolean reply(final RpcMessage m) {
            return sendRpc(remote, m, remoteHandlerId, null, 0);
        }

        @Override
        public boolean reply(final RpcMessage m, final RpcHandler replyHandler) {
            return sendRpc(remote, m, remoteHandlerId, replyHandler, replyHandler == null ? 0 : Integer.MAX_VALUE);
        }

        @Override
        public boolean reply(final RpcMessage m, final RpcHandler replyHandler, int timeout) {
            return sendRpc(remote, m, remoteHandlerId, replyHandler, replyHandler == null ? 0 : timeout);
        }

        @Override
        public void dispose() {
            Thread.dumpStack();
        }
    }

    MultiplexedConnection getOutgoingMultiplexedConnection(Endpoint remote) {
        synchronized (Threading.lockFor(remote)) {

            MultiplexedConnection res = connections.get(remote);
            try {
                if (res == null || res.connection.failed()) {
                    res = new MultiplexedConnection(endpoint.connect(remote));
                    connections.put(remote, res);
                    Log.finest("New conn:" + res.connection.remoteEndpoint() + " local:"
                            + res.connection.localEndpoint());
                } else {
                    Log.finest("ReUsing " + res.connection.remoteEndpoint());
                }
                res.touch();
            } catch (Throwable t) {
                // t.printStackTrace();
            }
            return res;
        }
    }

    MultiplexedConnection createIncomingMultiplexedConnection(TcpConnection conn) {
        synchronized (connections) {
            Log.finest("Incoming:" + conn.remoteEndpoint() + " local:" + conn.localEndpoint());
            MultiplexedConnection res = new MultiplexedConnection(conn);
            MultiplexedConnection old = connections.get(conn.remoteEndpoint());
            if (old == null || old.connection.failed()) {
                old = connections.put(conn.remoteEndpoint(), res);
                if (old != null)
                    old.connection.dispose();
                return res;
            } else
                return old;
        }
    }

    @Override
    public void onReceive(TcpConnection conn, Message m) {
        throw new NetworkingException("Incoming object is not an RpcPacket???");
    }

    @Override
    public void onFailure(Endpoint dst, Message m) {
        Thread.dumpStack();
    }

    class RPC_Handlers {
        static final long MAX_SERVICE = 1L << 16;

        final static double GC_DURATION = 300.0;

        final int timeout;
        final long handlerId;
        
        double expiration;
        final RpcHandler handler;

        volatile RpcPacket reply;

        // need to gc handlers that do not get a reply. Quick/dirty solution is
        // to
        // delete those old enough...
 
        public RPC_Handlers(RpcHandler handler, int timeout) {
            this.handler = handler;
            this.timeout = timeout;
            expiration = Sys.currentTime() + 0.001 * timeout;
            synchronized (handlers) {
                handlerId = ++g_handlers;
                handlers.put(handlerId, this);
            }
        }

        public RPC_Handlers(int service, RpcHandler handler) {
            timeout = -1;
            this.handler = handler;
            expiration = Double.MAX_VALUE;
            synchronized (handlers) {
                handlerId = service;
                handlers.put(handlerId, this);
            }
        }

        void touch() {
            if( this.expiration < Double.MAX_VALUE )
                this.expiration = Sys.currentTime() + GC_DURATION;
        }
        
        boolean isServiceHandler() {
            return handlerId < MAX_SERVICE;
        }

        boolean isSynchronous() {
            return timeout > 0 && !isExpired();
        }

        boolean isExpired() {
            return Sys.currentTime() > expiration;
        }

        void setReply(RpcPacket reply) {
            this.reply = reply;
            synchronized (this) {
                Threading.notifyAllOn(this);
            }
        }

        boolean blockForReply() {
            while (isSynchronous() && reply == null) {
                Threading.synchronizedWaitOn(this, 5);
            }
            return reply != null;
        }
    }

    RPC_Handlers getHandler(long handlerId) {
        synchronized (handlers) {
            RPC_Handlers res = handlers.get(handlerId);
            if( res != null )
                res.touch();
            return res;
        }
    }

    void gcHandlers() {
        new PeriodicTask(0, RPC_Handlers.GC_DURATION / 5) {
            @Override
            public void run() {
                double now = Sys.currentTime();
                synchronized (handlers) {
                    for (Iterator<RPC_Handlers> it = handlers.values().iterator(); it.hasNext();) {
                        RPC_Handlers h = it.next();
                        if (now > h.expiration + RPC_Handlers.GC_DURATION) {
                            it.remove();
                            Log.finest("GC'ing reply handler:" + h.handler );
                        }
                    }
                }
            }
        };
    }
    
    static long g_handlers = RPC_Handlers.MAX_SERVICE;
    Map<Long, RPC_Handlers> handlers = new HashMap<Long, RPC_Handlers>();
}
