package sys.net.impl;

import static sys.net.api.Networking.Networking;
import static sys.utils.Log.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.NetworkingException;
import sys.net.api.Serializer;
import sys.net.api.TcpConnection;
import sys.scheduler.Task;
import sys.utils.Threading;

/**
 * Represent a local communication endpoint that listens for incomming messagens
 * and allows establishing connections to remote endpoints for supporting
 * message exchanges
 * 
 * @author smd
 * 
 */
public class LocalEndpoint extends AbstractEndpoint {

    final TcpServer tcpServer;

    protected LocalEndpoint(final int tcpPort) throws IOException {
        super();
        tcpServer = new TcpServer(tcpPort);
        locator = super.encodeLocator(InetAddress.getLocalHost(), tcpServer.getLocalPort());
    }

    static synchronized Endpoint open() {
        return open(0);
    }

    static synchronized Endpoint open(final int tcpPort) {
        try {
            return new LocalEndpoint(tcpPort);
        } catch (IOException x) {
            throw new NetworkingException(x);
        }
    }

    /**
     * Establishes a tcp connection to a remote endpoint.
     * 
     * @param dst
     *            Remote endpoint of the connection
     * @return an object representing the connection to the endpoint, allowing
     *         for message exchange.
     */
    @Override
    public TcpConnection connect(final Endpoint dst) {
        try {
            return new TcpConnectionImpl(this, dst).connect();
        } catch (Exception x) {
            x.printStackTrace();
            Log.log(Level.FINE, "LocalEndpoint.connect", x);
            Log.finest("Bad connection to:" + dst);
        }
        new Task(0) {
            @Override
            public void run() {
                try {
                    handler.onFailure(dst, null);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        };
        return null;
    }

    /**
     * Sends a message to a (remote) endpoint, after successfully establishing a
     * tcp connection.
     * 
     * @param dst
     *            endpoint that meant to receive the message
     * @param m
     *            the message being sent
     * @return the connection to the endpoint, allowing for further message
     *         exchange.
     */
    @Override
    synchronized public TcpConnection send(final Endpoint dst, final Message m) {
        try {
            return new TcpConnectionImpl(this, dst).tcpOpenAndSend(m);
        } catch (Exception x) {
            x.printStackTrace();
            Log.log(Level.FINE, "LocalEndpoint.send", x);
            Log.finest("Bad connection to:" + dst);
        }
        new Task(0) {
            @Override
            public void run() {
                try {
                    handler.onFailure(dst, m);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        };
        return null;
    }

    final private class TcpServer {

        private ServerSocket ss;

        private TcpServer(int port) throws IOException {
            ss = new ServerSocket(port);
            tcpAddress = new InetSocketAddress(InetAddress.getLocalHost(), ss.getLocalPort());

            Log.finest("Bound to: " + LocalEndpoint.this);

            Threading.newThread(true, new Runnable() {
                @Override
                public void run() {
                    try {
                        for (;;) {
                            new TcpConnectionImpl(LocalEndpoint.this, ss.accept());
                        }
                    } catch (IOException x) {
                        x.printStackTrace();
                    }
                }
            }).start();
        }

        int getLocalPort() {
            return ss.getLocalPort();
        }
    }

}

class TcpConnectionImpl implements TcpConnection {

    private static int CONNECTION_TIMEOUT = 5000;

    private Socket sock;
    private AbstractEndpoint local;
    private AbstractEndpoint remote;
    private DataInputStream dis;
    private DataOutputStream dos;

    private boolean failed = false;
    private Serializer serializer = new KryoSerializer();

    TcpConnectionImpl(AbstractEndpoint local, Endpoint remote) {
        this.local = local;
        this.remote = (AbstractEndpoint) remote;
    }

    TcpConnectionImpl(AbstractEndpoint local, Socket sock) {
        this.sock = sock;
        this.local = local;
        prepareIncomingConnection();
    }

    @Override
    public boolean failed() {
        return failed;
    }

    @Override
    public void dispose() {
        try {
            if (sock != null) {
                sock.close();
            }
        } catch (IOException ignored) {
        }
        sock = null;
        dis = null;
        dos = null;
    }

    TcpConnection connect() {
        try {
            sock = new Socket();
            sock.setTcpNoDelay(true);
            sock.connect(remote.tcpAddress(), CONNECTION_TIMEOUT);
            dis = new DataInputStream(sock.getInputStream());
            dos = new DataOutputStream(sock.getOutputStream());
            dos.writeInt(local.tcpAddress().getPort());
            Log.finest("Connected to:" + remote.tcpAddress());
            return this;
        } catch (Throwable x) {
            // x.printStackTrace();
            Log.log(Level.FINE, String.format("Connection refused: %s", remote));
            dispose();
            return new FailedTcpConnection(local, remote);
        }
    }

    TcpConnection tcpOpenAndSend(Message m) {
        TcpConnection res = connect();
        if (!res.failed())
            send(m);

        return res;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Message> T receive() {
        try {
            synchronized (dis) {
                return (T) serializer.readObject(dis);
            }
        } catch (Exception x) {
//            x.printStackTrace();
            Log.fine( String.format("Broken connection<read error>: %s", remote));
            dispose();
            failed = true;
            return null;
        }
    }

    @Override
    public boolean send(Message m) {
        try {
            synchronized (dos) {
                serializer.writeObject(dos, m);
            }
            return true;
        } catch (Exception x) {
//            x.printStackTrace();
            Log.fine( String.format("Broken connection<write error>: %s", remote));
            dispose();
            failed = true;
            return false;
        }
    }

    void prepareIncomingConnection() {
        try {
            sock.setTcpNoDelay(true);
            dis = new DataInputStream(sock.getInputStream());
            dos = new DataOutputStream(sock.getOutputStream());

            int remotePort = dis.readInt();

            InetSocketAddress raddr = (InetSocketAddress) sock.getRemoteSocketAddress();
            remote = new RemoteEndpoint(new InetSocketAddress(raddr.getAddress(), remotePort));

            synchronized (dis) {
                
                final Message msg = serializer.readObject(dis);

                new Task(0) {
                    public void run() {
                        try {
                            msg.deliverTo(TcpConnectionImpl.this, local.handler);
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    }
                };
            }

        } catch (Exception x) {
            x.printStackTrace();
            Log.log(Level.FINE, "TcpConnection.prepareIncomingConnection", x);
            dispose();
        }
    }

    @Override
    public Endpoint localEndpoint() {
        return local;
    }

    @Override
    public Endpoint remoteEndpoint() {
        return remote;
    }
}

class FailedTcpConnection implements TcpConnection {

    final Endpoint local;
    final Endpoint remote;

    FailedTcpConnection(Endpoint local, Endpoint remote) {
        this.local = local;
        this.remote = remote;
    }

    @Override
    public void dispose() {
    }

    @Override
    public boolean send(Message m) {
        throw new NetworkingException("Invalid connection state...");
    }

    @Override
    public <T extends Message> T receive() {
        throw new NetworkingException("Invalid connection state...");
    }

    @Override
    public boolean failed() {
        return true;
    }

    @Override
    public Endpoint localEndpoint() {
        return local;
    }

    @Override
    public Endpoint remoteEndpoint() {
        return remote;
    }
}