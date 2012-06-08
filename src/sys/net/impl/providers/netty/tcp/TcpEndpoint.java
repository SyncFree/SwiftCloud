package sys.net.impl.providers.netty.tcp;

import static sys.utils.Log.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractEndpoint;
import sys.net.impl.AbstractLocalEndpoint;
import sys.net.impl.providers.KryoBuffer;
import sys.net.impl.providers.KryoBufferPool;
import sys.net.impl.providers.LocalEndpointExchange;
import sys.net.impl.providers.RemoteEndpointUpdater;
import sys.utils.Threading;

import static sys.Sys.Sys;

public class TcpEndpoint extends AbstractLocalEndpoint {

	ExecutorService bossExecutors, workerExecutors;
	final KryoBufferPool writeBufferPool = new KryoBufferPool(64);

	public TcpEndpoint(Endpoint local, int tcpPort) throws IOException {
		this.localEndpoint = local;
		this.gid = Sys.rg.nextLong();
		
		bossExecutors = Executors.newCachedThreadPool();
		workerExecutors = Executors.newCachedThreadPool();

		if (tcpPort >= 0) {
			ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(bossExecutors, workerExecutors));

			// Set up the pipeline factory.
			bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
				public ChannelPipeline getPipeline() throws Exception {
					return Channels.pipeline(new MessageFrameDecoder(), new IncomingConnectionHandler());
				}
			});
			bootstrap.setOption("child.tcpNoDelay", true);
			bootstrap.setOption("child.keepAlive", true);
			Channel ch = bootstrap.bind(new InetSocketAddress(tcpPort));
			super.setSocketAddress(((InetSocketAddress) ch.getLocalAddress()).getPort());
			Log.finest("Bound to: " + this);
		} else
			super.setSocketAddress(0);
	}

	public void start() throws IOException {
		handler = localEndpoint.getHandler();

		while (this.writeBufferPool.remainingCapacity() > 0)
			this.writeBufferPool.offer(new KryoBuffer());

	}

	public TransportConnection connect(Endpoint remote) {
		final OutgoingConnectionHandler res = new OutgoingConnectionHandler( remote );
		ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(bossExecutors, workerExecutors));
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(new MessageFrameDecoder(), res);
			}
		});
		ChannelFuture future = bootstrap.connect(((AbstractEndpoint) remote).sockAddress());
		Threading.synchronizedWaitOn(res, 5000);
		if (!future.isSuccess()) {
			Log.severe("Bad connection to:" + remote);
			return null;
		} else
			return res;
	}

	class AbstractConnection extends SimpleChannelUpstreamHandler implements TransportConnection, RemoteEndpointUpdater {

		Channel channel;
		boolean failed;
		Endpoint remote;
		Throwable cause;

		@Override
		public boolean failed() {
			return failed;
		}

		public boolean send(final Message msg) {
			KryoBuffer outBuf = null;
			try {
				outBuf = writeBufferPool.take();
				outBuf.writeClassAndObjectFrame(msg);
				channel.write(ChannelBuffers.wrappedBuffer(outBuf.toByteBuffer())).awaitUninterruptibly();
				return true;
			} catch (Throwable t) {
				t.printStackTrace();
			} finally {
				if (outBuf != null)
					writeBufferPool.offer(outBuf);
			}
			return false;
		}

		@Override
		public <T extends Message> T receive() {
			Thread.dumpStack();
			return null;
		}

		@Override
		public Endpoint localEndpoint() {
			return localEndpoint;
		}

		@Override
		public Endpoint remoteEndpoint() {
			return remote;
		}

		@Override
		public void dispose() {
			channel.close();
			handler.onClose(this);
		}

		public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e) {
			workerExecutors.execute(new Runnable() {
				public void run() {
					try {
						Message msg = KryoBuffer.readClassAndObject(((ChannelBuffer) e.getMessage()).toByteBuffer());
						msg.deliverTo(AbstractConnection.this, handler);
					} catch (Throwable t) {
						t.printStackTrace();
					}
				}
			});
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
			failed = true;
			cause = e.getCause();
			e.getChannel().close();
			handler.onFailure(this);
		}

		@Override
		public Throwable causeOfFailure() {
			return cause;
		}

		public void setRemoteEndpoint(Endpoint remote) {
			this.remote = remote;
		}
	}

	class IncomingConnectionHandler extends AbstractConnection {

		synchronized public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
			channel = e.getChannel();
		}
	}

	class OutgoingConnectionHandler extends AbstractConnection {
		public OutgoingConnectionHandler(Endpoint remote) {
			super.remote = remote;
		}
		
		synchronized public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
			channel = e.getChannel();
			workerExecutors.execute( new Runnable() {
				public void run() {
					send(new LocalEndpointExchange(localEndpoint));					
				}
			});
			handler.onConnect(this);
			Threading.synchronizedNotifyAllOn( this );
		}
		
		public String toString() {
			return "" + localEndpoint + " -> " + remote + ": " + channel.getLocalAddress() ;
		}
	}

	class MessageFrameDecoder extends LengthFieldBasedFrameDecoder {
		public MessageFrameDecoder() {
			super(Integer.MAX_VALUE, 0, 4, 0, 4, false);
		}
	}
}