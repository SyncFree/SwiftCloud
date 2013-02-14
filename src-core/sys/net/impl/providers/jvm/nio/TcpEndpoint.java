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
package sys.net.impl.providers.jvm.nio;

import static sys.Sys.Sys;
import static sys.net.impl.NetworkingConstants.KRYOBUFFERPOOL_CLT_MAXSIZE;
import static sys.net.impl.NetworkingConstants.KRYOBUFFERPOOL_SRV_MAXSIZE;
import static sys.net.impl.NetworkingConstants.TCP_CONNECTION_TIMEOUT;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractEndpoint;
import sys.net.impl.AbstractLocalEndpoint;
import sys.net.impl.FailedTransportConnection;
import sys.net.impl.providers.AbstractTransport;
import sys.net.impl.providers.InitiatorInfo;
import sys.net.impl.providers.RemoteEndpointUpdater;
import sys.net.impl.providers.jvm.BufferPool;
import sys.net.impl.providers.jvm.KryoBuffer;
import sys.utils.IO;
import sys.utils.Threading;

import com.esotericsoftware.kryo.KryoException;

final public class TcpEndpoint extends AbstractLocalEndpoint implements Runnable {

    private static Logger Log = Logger.getLogger(TcpEndpoint.class.getName());

    ServerSocketChannel ssc;
    BufferPool bufferPool;

    public TcpEndpoint(Endpoint local, int tcpPort) throws IOException {
        this.localEndpoint = local;
        this.gid = Sys.rg.nextLong() >>> 1;

        if (tcpPort >= 0) {
            ssc = ServerSocketChannel.open();
            ssc.socket().bind(new InetSocketAddress(tcpPort));
            bufferPool = new BufferPool(KRYOBUFFERPOOL_SRV_MAXSIZE);
        } else
            bufferPool = new BufferPool(KRYOBUFFERPOOL_CLT_MAXSIZE);

        super.setSocketAddress(ssc == null ? 0 : ssc.socket().getLocalPort());
    }

    public void start() throws IOException {

        handler = localEndpoint.getHandler();

        if (ssc != null)
            Threading.newThread("accept", true, this).start();
    }

    public TransportConnection connect(Endpoint remote) {
        try {
            if (((AbstractEndpoint) remote).isIncoming())
                return new OutgoingConnection(remote);
            else {
                Log.info("Attempting to connect to an outgoing only endpoint. " + remote);
            }
            return new FailedTransportConnection(localEndpoint, remote, null);
        } catch (Throwable t) {
            Log.log(Level.WARNING, "Cannot connect to: <" + remote + "> :" + t.getMessage());
            return new FailedTransportConnection(localEndpoint, remote, t);
        }
    }

    @Override
    public void run() {
        try {
            Log.finest("Bound to: " + this);
            for (;;) {
                SocketChannel channel = ssc.accept();
                configureChannel(channel);
                new IncomingConnection(channel);
            }
        } catch (Exception x) {
            Log.log(Level.SEVERE, "Unexpected error in incoming endpoint: " + localEndpoint, x);
        }
        IO.close(ssc);
    }

    static void configureChannel(SocketChannel ch) {
        try {
            ch.socket().setTcpNoDelay(true);
            ch.socket().setReceiveBufferSize(1 << 20);
            ch.socket().setSendBufferSize(1500);
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    abstract class AbstractConnection extends AbstractTransport implements RemoteEndpointUpdater, Runnable {

        String type;
        Throwable cause;
        SocketChannel channel;

        public AbstractConnection() throws IOException {
            super(localEndpoint, null);
        }

        @Override
        final public void run() {

            KryoBuffer inBuf = null;
            try {
                for (;;) {
                    try {
                        inBuf = bufferPool.poll();

                        Message msg = inBuf.readFrom(channel);

                        if (msg != null) {
                            Sys.downloadedBytes.addAndGet(msg.getSize());
                            incomingBytesCounter.addAndGet(msg.getSize());
                            msg.deliverTo(this, TcpEndpoint.this.handler);
                        }
                    } catch (Exception x) {
                        x.printStackTrace();
                    } finally {
                        bufferPool.offer(inBuf);
                    }
                }
            } catch (Throwable t) {
                // t.printStackTrace();
                Log.log(Level.FINEST, "Exception in connection to: " + remote, t);
                cause = t;
                handler.onFailure(this);
            }
            isBroken = true;
            IO.close(channel);
            Log.fine("Closed connection to: " + remote);
        }

        final public boolean send(final Message msg) {
            try {
                KryoBuffer outBuf = bufferPool.poll();
                try {
                    int msgSize = outBuf.writeClassAndObject(msg, channel);
                    Sys.uploadedBytes.getAndAdd(msgSize);
                    outgoingBytesCounter.getAndAdd(msgSize);
                    msg.setSize(msgSize);
                    return true;
                } catch (Exception x) {
                    x.printStackTrace();
                } finally {
                    bufferPool.offer(outBuf);
                }
            } catch (Throwable t) {
                if (t instanceof KryoException)
                    Log.log(Level.SEVERE, "Exception in connection to: " + remote, t);
                else
                    Log.log(Level.WARNING, "Exception in connection to: " + remote, t);

                cause = t;
                isBroken = true;
                IO.close(channel);
                handler.onFailure(this);
            }
            return false;
        }

        @Override
        public Throwable causeOfFailure() {
            return failed() ? cause : new Exception("?");
        }

        public String toString() {
            return String.format("%s (%s->%s)", type, channel.socket().getLocalPort(), channel.socket()
                    .getRemoteSocketAddress());
        }

        public void setOption(String op, Object val) {
            super.setOption(op, val);
        }

        public void setRemoteEndpoint(Endpoint remote) {
            this.remote = remote;
        }
    }

    class IncomingConnection extends AbstractConnection {

        public IncomingConnection(SocketChannel channel) throws IOException {
            super.channel = channel;
            super.type = "in";
            Threading.newThread("incoming-tcp-channel-reader:" + local + " <-> " + remote, true, this).start();
        }
    }

    class OutgoingConnection extends AbstractConnection implements Runnable {

        public OutgoingConnection(Endpoint remote) throws IOException {
            super.setRemoteEndpoint(remote);
            super.type = "out";
            init();
        }

        void init() throws IOException {
            try {
                channel = SocketChannel.open();
                channel.socket().connect(((AbstractEndpoint) remote).sockAddress(), TCP_CONNECTION_TIMEOUT);
                configureChannel(channel);
            } catch (IOException x) {
                cause = x;
                isBroken = true;
                IO.close(channel);
                throw x;
            }
            this.send(new InitiatorInfo(localEndpoint));
            handler.onConnect(this);
            Threading.newThread("outgoing-tcp-channel-reader:" + local + " <-> " + remote, true, this).start();
        }
    }
}