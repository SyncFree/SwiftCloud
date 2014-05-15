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
package sys.net.impl.providers.oio;

import static sys.Sys.Sys;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractEndpoint;
import sys.net.impl.AbstractLocalEndpoint;
import sys.net.impl.FailedTransportConnection;
import sys.net.impl.KryoLib;
import sys.net.impl.providers.AbstractTransport;
import sys.net.impl.providers.InitiatorInfo;
import sys.net.impl.providers.RemoteEndpointUpdater;
import sys.utils.IO;
import sys.utils.Threading;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

final public class TcpEndpoint extends AbstractLocalEndpoint implements Runnable {

    private static Logger Log = Logger.getLogger(TcpEndpoint.class.getName());

    ServerSocketChannel ssc;

    public TcpEndpoint(Endpoint local, int tcpPort) throws IOException {
        this.localEndpoint = local;
        this.gid = Sys.rg.nextLong() >>> 1;

        if (tcpPort >= 0) {
            ssc = ServerSocketChannel.open();
            ssc.socket().bind(new InetSocketAddress(tcpPort));
        }
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
            t.printStackTrace();
            Log.log(Level.WARNING, "Cannot connect to: <" + remote + "> :" + t.getMessage());
            return new FailedTransportConnection(localEndpoint, remote, t);
        }
    }

    @Override
    public void run() {
        try {
            Log.finest("Bound to: " + this);
            for (;;) {
                SocketChannel cs = ssc.accept();
                new IncomingConnection(cs);
            }
        } catch (Exception x) {
            Log.log(Level.SEVERE, "Unexpected error in incoming endpoint: " + localEndpoint, x);
        } finally {
            IO.close(ssc);
        }
    }

    static void configureChannel(Socket cs) {
        try {
            cs.setTcpNoDelay(true);
            cs.setReceiveBufferSize(1 << 20);
            cs.setSendBufferSize(1 << 20);
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    abstract class AbstractConnection extends AbstractTransport implements RemoteEndpointUpdater, Runnable {

        String type;
        Throwable cause;
        Socket socket;
        SocketChannel channel;
        KryoInputBuffer inBuf;
        KryoOutputBuffer outBuf;
        ExecutorService workers = Executors.newFixedThreadPool(2);

        public AbstractConnection() throws IOException {
            super(localEndpoint, null);
        }

        @Override
        final public void run() {
            try {
                for (;;) {
                    Message msg;
                    synchronized (inBuf) {
                        msg = inBuf.readClassAndObject(channel);
                        int msgSize = inBuf.msgSize;
                        Sys.downloadedBytes.addAndGet(msgSize);
                        incomingBytesCounter.addAndGet(msgSize);
                        msg.setSize(msgSize);
                    }
                    msg.deliverTo(this, TcpEndpoint.this.handler);
                }
            } catch (RuntimeException x) {
            } catch (IOException x) {
                Log.warning("Exception in connection to: " + remote + "/" + x.getMessage());
                cause = x;
                handler.onFailure(this);
            } catch (Throwable t) {
                Log.severe(t.getMessage());
                t.printStackTrace();
                cause = t;
            }
            isBroken = true;
            IO.close(socket);
            Log.fine("Closed connection to: " + remote);
        }

        synchronized public boolean send(final Message msg) {
            try {
                int msgSize = outBuf.writeClassAndObject(msg, channel);
                Sys.uploadedBytes.getAndAdd(msgSize);
                outgoingBytesCounter.getAndAdd(msgSize);
                msg.setSize(msgSize);
                return true;
            } catch (Throwable t) {
                if (Log.isLoggable(Level.INFO))
                    t.printStackTrace();

                Log.warning("Exception in connection to: " + remote + " " + t.getMessage());

                cause = t;
                isBroken = true;
                IO.close(socket);
                handler.onFailure(this);
            }
            return false;
        }

        @Override
        public Throwable causeOfFailure() {
            return failed() ? cause : new Exception("?");
        }

        public String toString() {
            return String.format("%s (%s->%s)", type, socket.getLocalPort(), socket.getRemoteSocketAddress());
        }

        public void setRemoteEndpoint(Endpoint remote) {
            this.remote = remote;
        }
    }

    final class IncomingConnection extends AbstractConnection {

        public IncomingConnection(SocketChannel channel) throws IOException {
            super.type = "in";
            super.channel = channel;
            super.socket = channel.socket();
            configureChannel(socket);
            inBuf = new KryoInputBuffer();
            outBuf = new KryoOutputBuffer();
            workers.execute(this);
        }
    }

    final class OutgoingConnection extends AbstractConnection implements Runnable {

        public OutgoingConnection(Endpoint remote) throws IOException {
            super.setRemoteEndpoint(remote);
            super.type = "out";
            init();
        }

        void init() throws IOException {
            try {
                channel = SocketChannel.open();
                channel.connect(((AbstractEndpoint) remote).sockAddress());
                socket = channel.socket();
                configureChannel(socket);
                inBuf = new KryoInputBuffer();
                outBuf = new KryoOutputBuffer();
            } catch (IOException x) {
                cause = x;
                isBroken = true;
                IO.close(socket);
                Log.warning("Cannot connect to: " + remote + " " + x.getMessage());
                if (Log.isLoggable(Level.INFO))
                    x.printStackTrace();
            }
            this.send(new InitiatorInfo(localEndpoint));
            handler.onConnect(this);
            workers.execute(this);
        }
    }
}

final class KryoInputBuffer {

    private static final int MAXUSES = 1024; // 2b replace in networking
                                             // constants...

    int uses = 0;
    Input in;
    ByteBuffer buffer;
    int msgSize;

    KryoInputBuffer() {
        buffer = ByteBuffer.allocate(8192);
        in = new Input(buffer.array());
    }

    public int msgSize() {
        return msgSize;
    }

    final public int readFrom(SocketChannel ch) throws IOException {

        buffer.clear().limit(4);
        while (buffer.hasRemaining() && ch.read(buffer) > 0)
            ;

        if (buffer.hasRemaining())
            return -1;

        msgSize = buffer.getInt(0);

        ensureCapacity(msgSize);

        buffer.clear().limit(msgSize);
        while (buffer.hasRemaining() && ch.read(buffer) > 0)
            ;

        if (buffer.hasRemaining())
            return -1;

        buffer.flip();
        msgSize += 4;
        return msgSize;
    }

    @SuppressWarnings("unchecked")
    public <T> T readClassAndObject(SocketChannel ch) throws Exception {
        if (readFrom(ch) > 0) {
            in.setPosition(0);
            // KryoLib.kryo().reset();
            return (T) KryoLib.kryo().readClassAndObject(in);
        } else
            throw new RuntimeException("Channel closed...");
    }

    private void ensureCapacity(int required) {
        if (required > buffer.array().length || ++uses > MAXUSES) {
            buffer = ByteBuffer.allocate(nextPowerOfTwo(required));
            in.setBuffer(buffer.array());
            uses = 0;
        }
    }

    static private int nextPowerOfTwo(int value) {
        if (value == 0)
            return 1;
        if ((value & value - 1) == 0)
            return value;
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        return value + 1;
    }
}

final class KryoOutputBuffer {

    private static final int MAXUSES = 1024;

    int uses;
    Output out;
    ByteBuffer buffer;

    public KryoOutputBuffer() {
        uses = Integer.MAX_VALUE;
        reset();
    }

    private void reset() {
        if (uses++ > MAXUSES) {
            buffer = ByteBuffer.allocate(8192);
            out = new Output(buffer.array(), Integer.MAX_VALUE);
            uses = 0;
        }
    }

    public int writeClassAndObject(Object object, SocketChannel ch) throws Exception {
        reset();
        out.setPosition(4);
        // KryoLib.kryo().reset();
        KryoLib.kryo().writeClassAndObject(out, object);
        int length = out.position();

        if (length > buffer.capacity())
            buffer = ByteBuffer.wrap(out.getBuffer());

        buffer.clear();
        buffer.putInt(0, length - 4);
        buffer.limit(length);

        return ch.write(buffer);
    }
}