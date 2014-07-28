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

import static sys.net.impl.NetworkingConstants.RPC_MAX_SERVICE_ID;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import sys.net.api.Endpoint;
import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;
import sys.net.api.rpc.RpcFactory;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoCopyable;

final public class RpcPacket extends AbstractRpcPacket implements KryoCopyable<RpcPacket> {

    private static Logger Log = Logger.getLogger(RpcPacket.class.getName());

    SynchronousQueue<AbstractRpcPacket> queue;

    RpcPacket() {
    }

    RpcPacket(RpcFactoryImpl fac, long service, RpcHandler handler) {
        this.fac = fac;
        this.timeout = 0;
        this.handler = handler;
        this.handlerId = service;
        this.replyHandlerId = service;
        fac.handlers.put(handlerId, this);
    }

    RpcPacket(RpcFactoryImpl fac, Endpoint remote, RpcMessage payload, RpcPacket handle, RpcHandler replyhandler,
            int timeout, boolean streamingReplies) {
        this.fac = fac;
        this.remote = remote;
        this.timeout = timeout;
        this.payload = payload;
        this.handler = replyhandler;
        this.handlerId = handle.replyHandlerId;

        if (replyhandler != null) {
            long id = g_handlers.incrementAndGet();
            this.replyHandlerId = streamingReplies ? -id : id;
            fac.handlers.put(this.replyHandlerId, this);
        } else
            this.replyHandlerId = 0L;
    }

    @Override
    public Endpoint localEndpoint() {
        return fac.facEndpoint;
    }

    @Override
    public RpcHandle send(Endpoint dst, RpcMessage msg, RpcHandler replyHandler, boolean streamingReplies) {
        return send(dst, msg, replyHandler, 0, streamingReplies);
    }

    @Override
    public RpcHandle send(Endpoint dst, RpcMessage msg, RpcHandler replyHandler, int timeout) {
        return send(dst, msg, replyHandler, timeout, false);
    }

    private RpcHandle send(Endpoint dst, RpcMessage msg, RpcHandler replyHandler, int timeout, boolean streamingReplies) {
        RpcPacket pkt = new RpcPacket(fac, dst, msg, this, replyHandler, timeout, streamingReplies);
        if (timeout != 0) {
            pkt.queue = new SynchronousQueue<AbstractRpcPacket>();
            if (pkt.sendRpcPacket(null, this) == true) {
                try {
                    pkt.reply = pkt.queue.poll(timeout, TimeUnit.MILLISECONDS);
                } catch (Exception x) {
                    x.printStackTrace();
                }
                if (pkt.reply != null)
                    pkt.reply.payload.deliverTo(pkt.reply, pkt.handler);
            }
            return pkt;
        } else {
            pkt.remote = dst;
            pkt.sendRpcPacket(null, this);
            return pkt;
        }
    }

    @Override
    public RpcHandle reply(RpcMessage msg, RpcHandler replyHandler, int timeout) {
        if (this.replyHandlerId == 0L)
            return null;

        RpcPacket pkt = new RpcPacket(fac, remote(), msg, this, replyHandler, timeout, false);
        if (timeout != 0) {
            pkt.queue = new SynchronousQueue<AbstractRpcPacket>();
            if (pkt.sendRpcPacket(conn, this) == true) {
                try {
                    pkt.reply = pkt.queue.poll(timeout, TimeUnit.MILLISECONDS);
                } catch (Exception x) {
                    x.printStackTrace();
                }
                if (pkt.reply != null)
                    pkt.reply.payload.deliverTo(pkt.reply, pkt.handler);
            }
            return pkt;
        } else {
            pkt.sendRpcPacket(conn, this);
            return pkt;
        }

    }

    final void deliver(AbstractRpcPacket pkt) {
        if (queue != null) {
            try {
                if (!queue.offer(pkt, timeout, TimeUnit.MILLISECONDS))
                    pkt.payload.deliverTo(pkt, this.handler);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            if (this.handler != null)
                pkt.payload.deliverTo(pkt, this.handler);
            else {
                Log.warning(String.format("Cannot handle RpcPacket: %s from %s, reason handler is null", pkt
                        .getPayload().getClass(), pkt.remote()));
            }
        }
    }

    private boolean sendRpcPacket(TransportConnection conn, AbstractRpcPacket handle) {
        try {
            if (conn != null && conn.send(this) || fac.conMgr.send(remote(), this)) {
                // RpcStats.logSentRpcPacket(this, remote());
                payload = null;
                return true;
            } else {
                failed = true;
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
            else if (handle.handler != null)
                handle.handler.onFailure(this);

            return false;
        }
    }

    public String toString2() {
        return String.format("RPC(%s,%s,%s) : %s ", handlerId, replyHandlerId, this.handler, this.payload == null ? ""
                : payload.getClass());
    }

    public String toString() {
        return this.replyHandlerId + " "
                + (payload != null ? payload.getClass() : "null" + " >>>" + fac.conMgr.connections);
    }

    @Override
    public void deliverTo(TransportConnection conn, MessageHandler handler) {
        ((RpcFactoryImpl) handler).dispatch(conn, this);
    }

    // [0-MAX_SERVICE_ID[ are reserved for static service handlers.
    static AtomicLong g_handlers = new AtomicLong(RPC_MAX_SERVICE_ID);

    @Override
    public RpcFactory getFactory() {
        return fac;
    }

    @Override
    public RpcPacket copy(Kryo k) {
        RpcPacket res = new RpcPacket();

        res.handlerId = this.handlerId;
        res.replyHandlerId = this.replyHandlerId;
        res.payload = this.payload;

        return res;
    }
}