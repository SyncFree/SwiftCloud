package sys.net.impl.providers.netty.ws;

import static sys.utils.Log.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
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
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpRequestEncoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketVersion;
import org.jboss.netty.util.CharsetUtil;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractEndpoint;
import sys.net.impl.AbstractLocalEndpoint;
import sys.net.impl.RemoteEndpoint;
import sys.net.impl.providers.KryoBuffer;
import sys.net.impl.providers.KryoBufferPool;

import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

public class WebSocketEndpoint extends AbstractLocalEndpoint {

	ExecutorService bossExecutors, workerExecutors;
	final KryoBufferPool writeBufferPool = new KryoBufferPool(16);

	public WebSocketEndpoint(Endpoint local, int tcpPort) throws IOException {

		this.localEndpoint = local;
		bossExecutors = Executors.newCachedThreadPool();
		workerExecutors = Executors.newCachedThreadPool();

		if (tcpPort >= 0) {
			ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(bossExecutors, workerExecutors));

			// Set up the pipeline factory.
			bootstrap.setPipelineFactory(new WebSocketServerPipelineFactory());
			bootstrap.setOption("child.tcpNoDelay", true);
			bootstrap.setOption("child.keepAlive", true);
			Channel ch = bootstrap.bind(new InetSocketAddress(tcpPort));
			this.tcpPort = ((InetSocketAddress) ch.getLocalAddress()).getPort();
		} else
			this.tcpPort = 0;
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
		InetSocketAddress rAddr = ((AbstractEndpoint) remote).tcpAddress();

		ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(bossExecutors, workerExecutors));

		Channel ch = null;

		try {

			URI uri = new URI("ws://" + rAddr.getAddress().getHostAddress() + ":" + rAddr.getPort() + "/swift");
			final WebSocketClientHandshaker handshaker = new WebSocketClientHandshakerFactory().newHandshaker(uri, WebSocketVersion.V13, null, false, Collections.<String, String> emptyMap());

			final OutgoingWebSocketHandler outgoing = new OutgoingWebSocketHandler(handshaker);

			bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
				public ChannelPipeline getPipeline() throws Exception {
					ChannelPipeline pipeline = Channels.pipeline();
					pipeline.addLast("decoder", new HttpResponseDecoder());
					pipeline.addLast("encoder", new HttpRequestEncoder());
					pipeline.addLast("ws-handler", outgoing);
					return pipeline;
				}
			});

			// Connect
			ChannelFuture future = bootstrap.connect(rAddr).awaitUninterruptibly().rethrowIfFailed();
			ch = future.getChannel();
			handshaker.handshake(ch).awaitUninterruptibly().rethrowIfFailed();
			return outgoing;
		} catch (Exception x) {
			x.printStackTrace();
		}
		return null;
	}

	public class WebSocketServerPipelineFactory implements ChannelPipelineFactory {
		public ChannelPipeline getPipeline() throws Exception {
			ChannelPipeline pipeline = Channels.pipeline();
			pipeline.addLast("decoder", new HttpRequestDecoder());
			pipeline.addLast("aggregator", new HttpChunkAggregator(65536));
			pipeline.addLast("encoder", new HttpResponseEncoder());
			pipeline.addLast("handler", new IncomingWebSocketHandler());
			return pipeline;
		}
	}

	class AbstractWebSocketTransportConnection extends SimpleChannelUpstreamHandler implements TransportConnection {
		private final String WEBSOCKET_PATH = "/swift";

		Channel channel;
		boolean failed;
		Endpoint remote;

		@Override
		public boolean failed() {
			return failed;
		}

		public boolean send(final Message msg) {
			KryoBuffer outBuf = null;
			try {
				outBuf = writeBufferPool.take();
				outBuf.writeClassAndObject(msg);
				WebSocketFrame frame = new BinaryWebSocketFrame(ChannelBuffers.wrappedBuffer(outBuf.toByteBuffer()));
				channel.write(frame).awaitUninterruptibly();
				
				System.out.println("   sa sa sa s");
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

		protected void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {
			ChannelBuffer buffer = frame.getBinaryData();
			Message msg = KryoBuffer.readClassAndObject( buffer.toByteBuffer() );
			if( msg != null)
				msg.deliverTo(this, handler);
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
			failed = true;
			e.getChannel().close();
			handler.onFailure(this);
		}
		
		protected String getWebSocketLocation(HttpRequest req) {
			return "ws://" + req.getHeader(HttpHeaders.Names.HOST) + WEBSOCKET_PATH;
		}
		
		protected void setChannel( Channel ch ) {
			this.channel = ch;
			InetSocketAddress raddr = (InetSocketAddress) channel.getRemoteAddress();
			remote = new RemoteEndpoint(raddr);
		}
	}

	class IncomingWebSocketHandler extends AbstractWebSocketTransportConnection {
		
		WebSocketServerHandshaker handshaker;

		IncomingWebSocketHandler() {
			
		}
		
		public void channelBound(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
			super.setChannel(e.getChannel());
			handler.onAccept(this);			
		}

		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
			if (handshaker != null ) {
				super.handleWebSocketFrame(ctx, (WebSocketFrame) e.getMessage());				
			} else
				handShake(ctx, (HttpRequest) e.getMessage() ) ;
		}
		
		private void handShake( ChannelHandlerContext ctx, HttpRequest req ) {
			WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(this.getWebSocketLocation(req), null, false);
			this.handshaker = wsFactory.newHandshaker(req);
			if (this.handshaker == null) {
				wsFactory.sendUnsupportedWebSocketVersionResponse(ctx.getChannel());
			} else {
				this.handshaker.handshake(ctx.getChannel(), req).addListener(WebSocketServerHandshaker.HANDSHAKE_LISTENER);
			}
		}
	}

	class OutgoingWebSocketHandler extends AbstractWebSocketTransportConnection {

		WebSocketClientHandshaker handshaker;

		OutgoingWebSocketHandler(WebSocketClientHandshaker handshaker) {
			this.handshaker = handshaker;
		}

		public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
			super.setChannel(e.getChannel());
			handler.onConnect(this);
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
			if (handshaker.isHandshakeComplete())
				super.handleWebSocketFrame(ctx, (WebSocketFrame) e.getMessage());
			else
				handshaker.finishHandshake(e.getChannel(), (HttpResponse) e.getMessage());
		}
	}
}