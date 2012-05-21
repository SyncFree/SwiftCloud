package sys.net.impl.providers.netty.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;

import sun.tools.tree.ThisExpression;
import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractEndpoint;
import sys.net.impl.AbstractLocalEndpoint;
import sys.net.impl.RemoteEndpoint;
import sys.net.impl.providers.KryoBuffer;
import sys.net.impl.providers.KryoBufferPool;

import static sys.utils.Log.*;

public class TcpEndpoint extends AbstractLocalEndpoint {

	ExecutorService bossExecutors, workerExecutors;
	final KryoBufferPool writeBufferPool = new KryoBufferPool(64);

	public TcpEndpoint(Endpoint local, int tcpPort) throws IOException {

		this.localEndpoint = local;
		bossExecutors = Executors.newCachedThreadPool();
		workerExecutors = Executors.newCachedThreadPool();

		if (tcpPort >= 0) {
			ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(bossExecutors, workerExecutors));

			// Set up the pipeline factory.
			bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
				public ChannelPipeline getPipeline() throws Exception {
					return Channels.pipeline( new MessageFrameDecoder(), new OutgoingConnectionHandler()  );
				}
			});
			bootstrap.setOption("child.tcpNoDelay", true);
			bootstrap.setOption("child.keepAlive", true);
			Channel ch = bootstrap.bind(new InetSocketAddress(tcpPort));
			this.tcpPort = ((InetSocketAddress) ch.getLocalAddress()).getPort();
		} else
			this.tcpPort = 2;
	}

	public void start() throws IOException {
		handler = localEndpoint.getHandler();

		while (this.writeBufferPool.remainingCapacity() > 0)
			this.writeBufferPool.offer(new KryoBuffer());

	}

	public int getLocalPort() {
		return tcpPort;
	}

	public TransportConnection connect(Endpoint remote) {
		final OutgoingConnectionHandler res = new OutgoingConnectionHandler();
		ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(bossExecutors, workerExecutors));
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(new MessageFrameDecoder(), res);
			}
		});
		ChannelFuture future = bootstrap.connect(((AbstractEndpoint) remote).tcpAddress());
		future.awaitUninterruptibly(5000);

		if (!future.isSuccess()) {
			Log.severe("Bad connection to:" + remote);
			return null;
		} else {
//			ByteBuffer locatorBuffer = ByteBuffer.allocate(4);
//			locatorBuffer.putInt(tcpPort);
//			locatorBuffer.flip();
//			future.getChannel().write(ChannelBuffers.wrappedBuffer(locatorBuffer));
			return res;
		}
	}

	class AbstractTransportConnection extends SimpleChannelUpstreamHandler implements TransportConnection {

		Channel channel;
		boolean failed;
		Endpoint remote;

		@Override
		public boolean failed() {
			// TODO Auto-generated method stub
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
			workerExecutors.execute( new Runnable() {
				public void run() {
					try {
					Message msg = KryoBuffer.readClassAndObject(((ChannelBuffer) e.getMessage()).toByteBuffer());
					msg.deliverTo(AbstractTransportConnection.this, handler);					
					} catch( Throwable t ) {
						t.printStackTrace();
					}
				}
			});
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
			failed = true;
			e.getChannel().close();
			handler.onFailure(this);
		}
	}

	class IncomingConnectionHandler extends AbstractTransportConnection {		
		IncomingConnectionHandler( Channel ch, InetSocketAddress raddr ) {
			channel = ch;
			remote = new RemoteEndpoint( raddr );
			handler.onAccept(this);	
		}
	}

	class OutgoingConnectionHandler extends AbstractTransportConnection {
		synchronized public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
			channel = e.getChannel();
			InetSocketAddress raddr = (InetSocketAddress) channel.getRemoteAddress();
			remote = new RemoteEndpoint(raddr);
			handler.onConnect(this);
		}
	}

	class LocatorFrameDecoder extends FrameDecoder {

		public LocatorFrameDecoder() {
			super(true);
		}

		@Override
		protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) {
			InetSocketAddress raddr = (InetSocketAddress) channel.getRemoteAddress();
			MessageFrameDecoder replacement = new MessageFrameDecoder();
			ctx.getPipeline().replace(this, "second", replacement ) ;
			ctx.getPipeline().addLast("last", new IncomingConnectionHandler( channel, raddr  ) ) ;
			if( buf.readableBytes() > 0) {
				return buf.readBytes( buf.readableBytes() ) ;
			} else
				return null;
		}
	}

	class MessageFrameDecoder extends LengthFieldBasedFrameDecoder {
		public MessageFrameDecoder() {
			super(Integer.MAX_VALUE, 0, 4, 0, 4, false);
		}
	}
}