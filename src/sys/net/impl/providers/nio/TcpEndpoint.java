package sys.net.impl.providers.nio;

import static sys.utils.Log.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractEndpoint;
import sys.net.impl.AbstractLocalEndpoint;
import sys.net.impl.AbstractTransport;
import sys.net.impl.RemoteEndpoint;
import sys.net.impl.providers.KryoBuffer;
import sys.net.impl.providers.KryoBufferPool;
import sys.utils.Threading;

public class TcpEndpoint extends AbstractLocalEndpoint implements Runnable {

	private static final int MAX_POOL_THREADS = 12;
	private static final int CORE_POOL_THREADS = 3;
	private static final int MAX_IDLE_THREAD_IMEOUT = 30;

	ServerSocketChannel ssc;

	final BlockingQueue<Runnable> holdQueue = new ArrayBlockingQueue<Runnable>(256);
	final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(CORE_POOL_THREADS, MAX_POOL_THREADS, MAX_IDLE_THREAD_IMEOUT, TimeUnit.SECONDS, holdQueue);
	final KryoBufferPool writePool;

	public TcpEndpoint(Endpoint local, int tcpPort) throws IOException {
		this.localEndpoint = (AbstractEndpoint) local;

		if (tcpPort >= 0) {
			ssc = ServerSocketChannel.open();
			ssc.socket().bind(new InetSocketAddress(tcpPort));
			this.tcpPort = ssc.socket().getLocalPort();
		} else
			this.tcpPort = 0;

		writePool = new KryoBufferPool(tcpPort > 0 ? 64 : 8);
	}

	public void start() throws IOException {
		handler = localEndpoint.getHandler();

		threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

		while (this.writePool.remainingCapacity() > 0)
			this.writePool.offer(new KryoBuffer());

		if (tcpPort > 0)
			Threading.newThread(true, this).start();
	}

	@Override
	public int getLocalPort() {
		return tcpPort <= 0 ? 0 : ssc.socket().getLocalPort();
	}

	public TransportConnection connect(Endpoint remote) {
		try {
			return new OutgoingConnection(localEndpoint, remote);
		} catch (IOException e) {
			// e.printStackTrace();
			Log.severe("Bad connection to:" + remote);
		}
		return null;
	}

	@Override
	public void run() {
		try {
			Log.finest("Bound to: " + localEndpoint);
			ByteBuffer locatorBuffer = ByteBuffer.allocate(4);

			for (;;) {
				SocketChannel channel = ssc.accept();

				channel.socket().setTcpNoDelay(true);

				locatorBuffer.clear();
				do {
					channel.read(locatorBuffer);
				} while (locatorBuffer.hasRemaining());

				int remotePort = locatorBuffer.getInt(0);

				InetSocketAddress raddr = (InetSocketAddress) channel.socket().getRemoteSocketAddress();
				Endpoint remote = new RemoteEndpoint(new InetSocketAddress(raddr.getAddress(), remotePort));
				new IncomingConnection(localEndpoint, remote, channel);
			}
		} catch (Exception x) {
			x.printStackTrace();
		}
	}

	abstract class AbstractConnection extends AbstractTransport implements Runnable {

		SocketChannel channel;
		final KryoBufferPool readPool;
		final SynchronousQueue<KryoBuffer> rq;

		public AbstractConnection(Endpoint local, Endpoint remote) throws IOException {
			super(local, remote);
			this.rq = new SynchronousQueue<KryoBuffer>();
			this.readPool = new KryoBufferPool(128);
		}

		abstract void init() throws IOException;

		@Override
		public void run() {
			try {
				while (this.readPool.remainingCapacity() > 0)
					this.readPool.offer(new _ReadBuffer());

				for (;;) {
					KryoBuffer inBuf = readPool.take();

					if (inBuf.readFrom(channel))
						inBuf.run();
					else {
						this.readPool.offer(inBuf);
						break;
					}
					// // if (inBuf != null && !rq.offer(inBuf))
					// // threadPool.execute(inBuf);
					//
					// inBuf.run();
				}

			} catch (IOException ioe) {
				isBroken = true;
				handler.onFailure(this);
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}

		class _ReadBuffer extends KryoBuffer {
			@Override
			public void run() {
				try {
					Message msg = super.readClassAndObject();
					readPool.offer(this);
					msg.deliverTo(AbstractConnection.this, TcpEndpoint.this.handler);
				} catch (Throwable t) {
					t.printStackTrace();
				}
			}
		}

		public boolean send(final Message m) {
			KryoBuffer outBuf = null;
			try {
				outBuf = writePool.take();
				outBuf.writeClassAndObjectFrame(m, channel);
				return true;
			} catch (Throwable t) {
				t.printStackTrace();
			} finally {
				if (outBuf != null)
					writePool.offer(outBuf);
			}
			return false;
		}

		public <T extends Message> T receive() {
			KryoBuffer inBuf = null;
			try {
				inBuf = rq.take();
				T msg = inBuf.readClassAndObject();
				return msg;

			} catch (Throwable t) {
				t.printStackTrace();
			} finally {
				if (inBuf != null)
					readPool.offer(inBuf);
			}
			return null;
		}

	}

	class IncomingConnection extends AbstractConnection {

		public IncomingConnection(Endpoint local, Endpoint remote, SocketChannel channel) throws IOException {
			super(local, remote);
			super.channel = channel;
			init();
		}

		@Override
		void init() throws IOException {
			handler.onAccept(this);
			Threading.newThread(true, this).start();
		}

	}

	class OutgoingConnection extends AbstractConnection implements Runnable {
		final private int CONNECTION_TIMEOUT = 5000;

		public OutgoingConnection(Endpoint local, Endpoint remote) throws IOException {
			super(local, remote);
			init();
		}

		void init() throws IOException {
			channel = SocketChannel.open();
			channel.socket().connect(((AbstractEndpoint) remote).tcpAddress(), CONNECTION_TIMEOUT);
			channel.socket().setTcpNoDelay(true);

			ByteBuffer locatorBuffer = ByteBuffer.allocate(4);
			locatorBuffer.putInt(tcpPort).flip();
			channel.write(locatorBuffer);

			handler.onConnect(this);
			Threading.newThread(true, this).start();
		}
	}
}
