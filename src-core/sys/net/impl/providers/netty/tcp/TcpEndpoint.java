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
package sys.net.impl.providers.netty.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

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
import sys.net.api.NetworkingException;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractEndpoint;
import sys.net.impl.AbstractLocalEndpoint;
import sys.net.impl.NetworkingConstants.NIO_ReadBufferDispatchPolicy;
import sys.net.impl.NetworkingConstants.NIO_ReadBufferPoolPolicy;
import sys.net.impl.NetworkingConstants.NIO_WriteBufferPoolPolicy;
import sys.net.impl.providers.KryoInputBuffer;
import sys.net.impl.providers.BufferPool;
import sys.net.impl.providers.InitiatorInfo;
import sys.net.impl.providers.KryoOutputBuffer;
import sys.net.impl.providers.RemoteEndpointUpdater;
import sys.utils.Threading;

import static sys.Sys.Sys;

public class TcpEndpoint extends AbstractLocalEndpoint {

	private static Logger Log = Logger.getLogger( TcpEndpoint.class.getName() );


	
	ExecutorService bossExecutors, workerExecutors;
	final BufferPool<KryoOutputBuffer> writePool;

	public TcpEndpoint(Endpoint local, int tcpPort) throws IOException {
		this.localEndpoint = local;
		this.gid = Sys.rg.nextLong() >>> 1;

		bossExecutors = Executors.newCachedThreadPool();
		workerExecutors = Executors.newFixedThreadPool(32);

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
			writePool = new BufferPool<KryoOutputBuffer>(64);

		} else {
			super.setSocketAddress(0);
			writePool = new BufferPool<KryoOutputBuffer>(4);
		}
	}

	public void start() throws IOException {
		handler = localEndpoint.getHandler();

		while (this.writePool.remainingCapacity() > 0)
			this.writePool.offer(new KryoOutputBuffer());

	}

	public TransportConnection connect(Endpoint remote) {
		final OutgoingConnectionHandler res = new OutgoingConnectionHandler(remote);
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
		NIO_ReadBufferPoolPolicy readPoolPolicy = NIO_ReadBufferPoolPolicy.POLLING;
		NIO_WriteBufferPoolPolicy writePoolPolicy = NIO_WriteBufferPoolPolicy.POLLING;
		NIO_ReadBufferDispatchPolicy execPolicy = NIO_ReadBufferDispatchPolicy.READER_EXECUTES;

		final BufferPool<KryoInputBuffer> readPool;

		AbstractConnection() {
			readPool = new BufferPool<KryoInputBuffer>();
			while (this.readPool.remainingCapacity() > 0)
				this.readPool.offer(new KryoInputBuffer());
		}

		@Override
		public boolean failed() {
			return failed;
		}

		public boolean send(final Message msg) {
			KryoOutputBuffer outBuf = null;
			try {
				if (writePoolPolicy == NIO_WriteBufferPoolPolicy.BLOCKING)
					outBuf = writePool.take();
				else {
					outBuf = writePool.poll();
					if (outBuf == null)
						outBuf = new KryoOutputBuffer();
				}
				outBuf.writeClassAndObjectFrame(msg);
				ChannelFuture fut = channel.write(ChannelBuffers.wrappedBuffer(outBuf.toByteBuffer()));
				fut.awaitUninterruptibly();
				return true;
			} catch (Throwable t) {
				t.printStackTrace();
			} finally {
				if (outBuf != null)
					writePool.offer(outBuf);
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
					KryoInputBuffer inBuf = null;
					try {
						if (readPoolPolicy == NIO_ReadBufferPoolPolicy.BLOCKING)
							inBuf = readPool.take();
						else {
							inBuf = readPool.take();
							if (inBuf == null)
								inBuf = new KryoInputBuffer();
						}

						Message msg = inBuf.readClassAndObject(((ChannelBuffer) e.getMessage()).toByteBuffer());
						msg.deliverTo(AbstractConnection.this, handler);
					} catch (Throwable t) {
						t.printStackTrace();
					} finally {
						if (inBuf != null)
							readPool.offer(inBuf);
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

		@Override
		public boolean sendNow(Message m) {
			throw new NetworkingException("Not Implemented...");
		}

		@Override
		public void setOption(String op, Object value) {
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
			workerExecutors.execute(new Runnable() {
				public void run() {
					send(new InitiatorInfo(localEndpoint));
				}
			});
			Threading.synchronizedNotifyAllOn(this);
			handler.onConnect(this);
		}

		public String toString() {
			return "" + localEndpoint + " -> " + remote + ": " + channel.getLocalAddress();
		}
	}

	class MessageFrameDecoder extends LengthFieldBasedFrameDecoder {
		public MessageFrameDecoder() {
			super(Integer.MAX_VALUE, 0, 4, 0, 4, false);
		}
	}
}