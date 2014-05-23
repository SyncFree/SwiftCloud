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
package sys.net.impl.providers.lz4_oio;

import static sys.Sys.Sys;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractEndpoint;
import sys.net.impl.AbstractLocalEndpoint;
import sys.net.impl.FailedTransportConnection;
import sys.net.impl.KryoLib;
import sys.net.impl.Lz4Lib;
import sys.net.impl.providers.AbstractTransport;
import sys.net.impl.providers.InitiatorInfo;
import sys.net.impl.providers.RemoteEndpointUpdater;
import sys.scheduler.PeriodicTask;
import sys.utils.IO;
import sys.utils.Threading;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

final public class TcpEndpoint extends AbstractLocalEndpoint implements Runnable {

    private static Logger Log = Logger.getLogger(TcpEndpoint.class.getName());

    public static AtomicLong totalU = new AtomicLong(1);
    public static AtomicLong totalC = new AtomicLong(1);

    static {
        new PeriodicTask(0, 1) {
            public void run() {
                System.err.printf("Compression Ratio: %.1f%%\n", 100 - 100.0 * totalC.get() / totalU.get());
            }
        };
    }
    ServerSocket ss;

    public TcpEndpoint(Endpoint local, int tcpPort) throws IOException {
        this.localEndpoint = local;
        this.gid = Sys.rg.nextLong() >>> 1;

        if (tcpPort >= 0) {
            ss = new ServerSocket(tcpPort);
        }
        super.setSocketAddress(ss == null ? 0 : ss.getLocalPort());

    }

    public void start() throws IOException {

        handler = localEndpoint.getHandler();

        if (ss != null)
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
                Socket cs = ss.accept();
                new IncomingConnection(cs);
            }
        } catch (Exception x) {
            Log.log(Level.SEVERE, "Unexpected error in incoming endpoint: " + localEndpoint, x);
        } finally {
            IO.close(ss);
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
        KryoInputBuffer inBuf;
        KryoOutputBuffer outBuf;
        ExecutorService workers = Executors.newFixedThreadPool(2);

        DataInputStream dis;
        DataOutputStream dos;

        public AbstractConnection() throws IOException {
            super(localEndpoint, null);
        }

        @Override
        final public void run() {
            try {
                for (;;) {
                    Message msg;
                    synchronized (inBuf) {
                        msg = inBuf.readClassAndObject(dis);
                        int msgSize = inBuf.msgSize;
                        Sys.downloadedBytes.addAndGet(msgSize);
                        incomingBytesCounter.addAndGet(msgSize);
                    }
                    msg.deliverTo(this, TcpEndpoint.this.handler);
                }
            } catch (Throwable t) {
                if (Log.isLoggable(Level.INFO))
                    t.printStackTrace();
                Log.log(Level.FINEST, "Exception in connection to: " + remote, t);
                cause = t;
                handler.onFailure(this);
            }
            isBroken = true;
            IO.close(socket);
            Log.fine("Closed connection to: " + remote);
        }

        synchronized public boolean send(final Message msg) {
            try {
                int msgSize = outBuf.writeClassAndObject(msg, dos);
                Sys.uploadedBytes.getAndAdd(msgSize);
                outgoingBytesCounter.getAndAdd(msgSize);
                msg.setSize(msgSize);
                return true;
            } catch (Throwable t) {
                if (Log.isLoggable(Level.INFO))
                    t.printStackTrace();

                Log.log(Level.INFO, "Exception in connection to: " + remote, t);

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

        public IncomingConnection(Socket socket) throws IOException {
            super.type = "in";
            super.socket = socket;
            configureChannel(socket);
            inBuf = new KryoInputBuffer();
            outBuf = new KryoOutputBuffer();
            dis = new DataInputStream(socket.getInputStream());
            dos = new DataOutputStream(socket.getOutputStream());
            workers.execute(this);
            // Threading.newThread("incoming-tcp-channel-reader:" + local +
            // " <-> " + remote, true, this).start();
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
                socket = new Socket(remote.getHost(), remote.getPort());
                configureChannel(socket);
                inBuf = new KryoInputBuffer();
                outBuf = new KryoOutputBuffer();
                dis = new DataInputStream(socket.getInputStream());
                dos = new DataOutputStream(socket.getOutputStream());

            } catch (IOException x) {
                cause = x;
                isBroken = true;
                IO.close(socket);
                throw x;
            }
            this.send(new InitiatorInfo(localEndpoint));
            handler.onConnect(this);
            workers.execute(this);
            // Threading.newThread("outgoing-tcp-channel-reader:" + local +
            // " <-> " + remote, true, this).start();
        }
    }

}

final class KryoInputBuffer {

    Input in;
    int msgSize;

    KryoInputBuffer() {
    }

    public int msgSize() {
        return msgSize;
    }

    @SuppressWarnings("unchecked")
    public <T> T readClassAndObject(DataInputStream dis) throws Exception {
        msgSize = dis.readInt();
        int originalSize = dis.readInt();
        byte[] tmp = new byte[msgSize - 4];
        dis.readFully(tmp);
        byte[] res = Lz4Lib.lz4Decompressor().decompress(tmp, originalSize);
        return (T) KryoLib.kryo().readClassAndObject(new Input(res));
    }
}

final class KryoOutputBuffer {

    Output out;
    byte[] tmp = new byte[1 << 20];

    public KryoOutputBuffer() {
    }

    public int writeClassAndObject(Object object, DataOutputStream dos) throws Exception {

        Output out = new Output(tmp);
        KryoLib.kryo().writeClassAndObject(out, object);
        int length = out.position();

        TcpEndpoint.totalU.addAndGet(length);

        byte[] res = Lz4Lib.lz4Compressor().compress(tmp, 0, length);

        TcpEndpoint.totalC.addAndGet(res.length);

        dos.writeInt(res.length + 4);
        dos.writeInt(length);
        dos.write(res);
        return res.length + 4;
    }

}