package sys.net.impl.providers.nio;

import static sys.Sys.Sys;
import static sys.net.impl.NetworkingConstants.NIO_CONNECTION_TIMEOUT;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractEndpoint;
import sys.net.impl.AbstractLocalEndpoint;
import sys.net.impl.FailedTransportConnection;
import sys.net.impl.NetworkingConstants.NIO_ReadBufferDispatchPolicy;
import sys.net.impl.NetworkingConstants.NIO_ReadBufferPoolPolicy;
import sys.net.impl.NetworkingConstants.NIO_WriteBufferPoolPolicy;
import sys.net.impl.providers.AbstractTransport;
import sys.net.impl.providers.BufferPool;
import sys.net.impl.providers.InitiatorInfo;
import sys.net.impl.providers.KryoInputBuffer;
import sys.net.impl.providers.KryoOutputBuffer;
import sys.net.impl.providers.MultiQueueExecutor;
import sys.net.impl.providers.RemoteEndpointUpdater;
import sys.utils.IO;
import sys.utils.Threading;

import com.esotericsoftware.kryo.KryoException;

public class TcpEndpoint extends AbstractLocalEndpoint implements Runnable {

    private static Logger Log = Logger.getLogger(TcpEndpoint.class.getName());

    ServerSocketChannel ssc;
    MultiQueueExecutor executor = new MultiQueueExecutor();

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
                Log.info("Attempting to connect to an outgoing only endpoint." + remote);
            }
            return new FailedTransportConnection(localEndpoint, remote, null);
        } catch (Throwable t) {
            Log.log(Level.WARNING, "Cannot connect to: <" + remote + "> :" + t.getMessage(), t);
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
        final BufferPool<KryoInputBuffer> readPool;
        final BufferPool<KryoOutputBuffer> writePool;

        NIO_ReadBufferPoolPolicy readPoolPolicy = NIO_ReadBufferPoolPolicy.POLLING;
        NIO_WriteBufferPoolPolicy writePoolPolicy = NIO_WriteBufferPoolPolicy.POLLING;
        NIO_ReadBufferDispatchPolicy execPolicy = NIO_ReadBufferDispatchPolicy.READER_EXECUTES;

        public AbstractConnection() throws IOException {
            super(localEndpoint, null);
            this.readPool = new BufferPool<KryoInputBuffer>();
            this.writePool = new BufferPool<KryoOutputBuffer>();
        }

        @Override
        final public void run() {

            while (this.readPool.remainingCapacity() > 0)
                this.readPool.offer(new _ReadBuffer());

            while (this.writePool.remainingCapacity() > 0)
                this.writePool.offer(new KryoOutputBuffer());

            try {
                for (;;) {
                    KryoInputBuffer inBuf;

                    if (readPoolPolicy == NIO_ReadBufferPoolPolicy.BLOCKING)
                        inBuf = readPool.take();
                    else {
                        inBuf = readPool.poll();
                        if (inBuf == null)
                            inBuf = new _ReadBuffer();
                    }

                    if (inBuf.readFrom(channel)) {

                        if (execPolicy == NIO_ReadBufferDispatchPolicy.USE_THREAD_POOL)
                            executor.execute(this, inBuf);
                        else
                            inBuf.run();

                    } else {
                        this.readPool.offer(inBuf);
                        handler.onClose(this);
                        break;
                    }
                }
            } catch (Throwable t) {
                Log.log(Level.FINEST, "Exception in connection to" + remote, t);
                // t.printStackTrace();
                cause = t;
                handler.onFailure(this);
            }
            isBroken = true;
            IO.close(channel);
            Log.fine("Closed connection to: " + remote);
        }

        final class _ReadBuffer extends KryoInputBuffer {
            @Override
            public void run() {
                Message msg;
                try {
                    msg = super.readClassAndObject();
                    msg.setSize(super.contentLength);
                } catch (Throwable t) {
                    Log.log(Level.SEVERE, "Exception in connection to" + remote, t);
                    return;

                } finally {
                    readPool.offer(this);
                }
                try {
                    msg.deliverTo(AbstractConnection.this, TcpEndpoint.this.handler);
                } catch (Throwable t) {
                    Log.log(Level.WARNING,"Dispatch Exception: " + t.getClass() + " caused by: " + msg.getClass()
                            + " received from: " + remoteEndpoint() + " with " + super.contentLength, t);
                }
            }
        }

        final public boolean send(final Message m) {
            KryoOutputBuffer outBuf = null;
            try {
                if (writePoolPolicy == NIO_WriteBufferPoolPolicy.BLOCKING)
                    outBuf = writePool.take();
                else {
                    outBuf = writePool.poll();
                    if (outBuf == null)
                        outBuf = new KryoOutputBuffer();
                }
                int size = outBuf.writeClassAndObjectFrame(m, channel);
                m.setSize(size);
                return true;
            } catch (Throwable t) {
                
                if (t instanceof KryoException)
                    Log.log(Level.SEVERE, "Exception in connection to" + remote, t);
                else
                    Log.log(Level.WARNING, "Exception in connection to" + remote, t);

                cause = t;
                isBroken = true;
                IO.close(channel);
                handler.onFailure(this);
            } finally {
                if (outBuf != null)
                    writePool.offer(outBuf);
            }
            return false;
        }

        public boolean sendNow(final Message m) {

            try {
                KryoOutputBuffer outBuf = new KryoOutputBuffer();
                int size = outBuf.writeClassAndObjectFrame(m, channel);
                m.setSize(size);
                return true;
            } catch (Throwable t) {
                Log.log(Level.SEVERE, "Exception in connection to" + remote, t);
            }
            return false;
        }

        public <T extends Message> T receive() {
            throw new RuntimeException("Not implemented...");
            // KryoBuffer inBuf = null;
            // try {
            // inBuf = rq.take();
            // T msg = inBuf.readClassAndObject();
            // return msg;
            //
            // } catch (Throwable t) {
            // t.printStackTrace();
            // } finally {
            // if (inBuf != null)
            // readPool.offer(inBuf);
            // }
            // return null;
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

            if (op.equals("NIO_ReadBufferPoolPolicy"))
                readPoolPolicy = (NIO_ReadBufferPoolPolicy) val;
            else if (op.equals("NIO_ReadBufferPoolPolicy"))
                execPolicy = (NIO_ReadBufferDispatchPolicy) val;
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
                channel.socket().connect(((AbstractEndpoint) remote).sockAddress(), NIO_CONNECTION_TIMEOUT);
                configureChannel(channel);
            } catch (IOException x) {
                Log.log(Level.FINEST, "Exception in connection to" + remote, x);

                cause = x;
                isBroken = true;
                IO.close(channel);
                throw x;
            }
            this.send(new InitiatorInfo(localEndpoint));
            handler.onConnect(this);
            Threading.newThread("outgoing-tcp-channel-reader:" + local + " <-> " + remote, true, this).start();

            // new Task(Sys.rg.nextDouble() * 10) {
            // public void run() {
            // sendNow(new TcpPing( Sys.currentTime() ) );
            // this.reSchedule(5 + Sys.rg.nextDouble() * 10);
            // }
            // };
        }
    }
}