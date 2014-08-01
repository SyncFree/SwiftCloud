package sys.net.impl.providers.inproc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractLocalEndpoint;
import sys.net.impl.KryoLib;
import sys.net.impl.providers.AbstractTransport;
import sys.utils.Threading;

final public class InProcEndpoint extends AbstractLocalEndpoint {
    static AtomicInteger g_ephemerals = new AtomicInteger(0);
    static ExecutorService executor = Executors.newFixedThreadPool(8);

    public InProcEndpoint(Endpoint local, int port) throws IOException {
        this.handler = local.getHandler();
        if (port > 0)
            super.setSocketAddress(new InetSocketAddress("127.0.0.1", port));
        else
            super.setSocketAddress(new InetSocketAddress("127.0.0.1", g_ephemerals.incrementAndGet()));

        this.localEndpoint = this;
        endpoints.put(this.localEndpoint.getPort(), this);
    }

    public void start() throws IOException {
    }

    public TransportConnection connect(Endpoint remote) {
        InProcEndpoint r;
        do {
            r = endpoints.get(remote.getPort());
            Threading.sleep(1000);
        } while (r == null);

        InProcConnection lC = new InProcConnection(localEndpoint, remote, r.localEndpoint.getHandler());
        InProcConnection rC = new InProcConnection(remote, localEndpoint, localEndpoint.getHandler());

        lC.remoteEnd = rC;
        rC.remoteEnd = lC;

        localEndpoint.getHandler().onConnect(lC);
        r.localEndpoint.getHandler().onConnect(rC);

        return lC;
    }

    static class InProcConnection extends AbstractTransport {

        InProcConnection remoteEnd;
        final MessageHandler handler;

        public InProcConnection(Endpoint local, Endpoint remote, MessageHandler handler) {
            super(local, remote);
            this.handler = handler;
        }

        synchronized public boolean send(Message msg) {
            final Message copy = KryoLib.copyShallow(msg);
            executor.execute(new Runnable() {
                public void run() {
                    copy.deliverTo(remoteEnd, handler);
                }
            });
            return true;
        }

        @Override
        public Throwable causeOfFailure() {
            return new Exception("?");
        }

        public String toString() {
            return String.format("%s (%s->%s)", getClass().toString(), localEndpoint(), remoteEndpoint());
        }
    }

    static ConcurrentHashMap<Integer, InProcEndpoint> endpoints = new ConcurrentHashMap<Integer, InProcEndpoint>();
}