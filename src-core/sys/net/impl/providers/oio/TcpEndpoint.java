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
import static sys.net.impl.NetworkingConstants.TCP_CONNECTION_TIMEOUT;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
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

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

final public class TcpEndpoint extends AbstractLocalEndpoint implements Runnable {

    private static Logger Log = Logger.getLogger(TcpEndpoint.class.getName());

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
                configureChannel(cs);
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
            cs.setSendBufferSize(1500);
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    abstract class AbstractConnection extends AbstractTransport implements RemoteEndpointUpdater, Runnable {

        String type;
        Throwable cause;
        Socket socket;

        Input in;
        OutputStream os;
        MessageOutputStream baos = new MessageOutputStream();
        Output out = new Output(baos);

        public AbstractConnection() throws IOException {
            super(localEndpoint, null);
        }

        @Override
        final public void run() {
            try {
                for (;;) {
                    int size = in.readInt() + 4;
                    Message msg = (Message) KryoLib.kryo().readClassAndObject(in);
                    Sys.downloadedBytes.addAndGet(size);
                    incomingBytesCounter.addAndGet(size);
                    msg.deliverTo(this, TcpEndpoint.this.handler);
                }
            } catch (Throwable t) {
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
                baos.reset();
                out.clear();
                KryoLib.kryo().writeClassAndObject(out, msg);
                out.flush();
                int msgSize = baos.flushContents(os);
                Sys.uploadedBytes.getAndAdd(msgSize);
                outgoingBytesCounter.getAndAdd(msgSize);
                msg.setSize(msgSize);
                return true;
            } catch (Throwable t) {
                if (t instanceof KryoException)
                    Log.log(Level.SEVERE, "Exception in connection to: " + remote, t);
                else
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
            super.socket = socket;
            super.type = "in";
            configureChannel(socket);
            os = socket.getOutputStream();
            in = new Input(socket.getInputStream());
            Threading.newThread("incoming-tcp-channel-reader:" + local + " <-> " + remote, true, this).start();
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
                socket = new Socket();
                socket.connect(((AbstractEndpoint) remote).sockAddress(), TCP_CONNECTION_TIMEOUT);
                configureChannel(socket);
                os = socket.getOutputStream();
                in = new Input(socket.getInputStream());
            } catch (IOException x) {
                cause = x;
                isBroken = true;
                IO.close(socket);
                throw x;
            }
            this.send(new InitiatorInfo(localEndpoint));
            handler.onConnect(this);
            Threading.newThread("outgoing-tcp-channel-reader:" + local + " <-> " + remote, true, this).start();
        }
    }

    class MessageOutputStream extends ByteArrayOutputStream {

        public void reset() {
            super.count = 4;
        }

        public int flushContents(OutputStream os) throws IOException {
            int frameSize = count - 4;
            buf[0] = (byte) (frameSize >>> 24);
            buf[1] = (byte) ((frameSize >>> 16) & 0xFF);
            buf[2] = (byte) ((frameSize >>> 8) & 0xFF);
            buf[3] = (byte) ((frameSize) & 0xFF);
            os.write(buf, 0, count);
            os.flush();
            return count;
        }
    };
}
