package sys.net.impl.providers.netty;

import static sys.Sys.Sys;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractLocalEndpoint;
import sys.net.impl.KryoLib;
import sys.net.impl.providers.InitiatorInfo;
import sys.net.impl.providers.RemoteEndpointUpdater;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class TcpEndpoint extends AbstractLocalEndpoint {

    Endpoint localEndpoint;
    EventLoopGroup bossGroup;
    EventLoopGroup workerGroup;

    public TcpEndpoint(Endpoint local, int tcpPort) throws IOException {
        this.localEndpoint = local;
        this.gid = Sys.rg.nextLong() >>> 1;

        boolean isServer = tcpPort >= 0;

        if (isServer) {
            bind(tcpPort);
        } else {
            super.setSocketAddress(0);
        }
    }

    @Override
    public void start() throws Exception {
        handler = localEndpoint.getHandler();
    }

    void bind(int tcpPort) {
        try {
            bossGroup = new NioEventLoopGroup();
            workerGroup = new NioEventLoopGroup();

            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.config().setAutoRead(true);
                            TcpChannel channel = new TcpChannel(ch);
                            ch.pipeline().addLast(channel, new KryoMessageEncoder());
                        }
                    }).option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true);

            b.bind(tcpPort).sync().awaitUninterruptibly();
            super.setSocketAddress(tcpPort);
        } catch (Throwable t) {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            t.printStackTrace();
        }
    }

    @Override
    public TransportConnection connect(final Endpoint remote) {
        try {
            workerGroup = new NioEventLoopGroup();

            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true).option(ChannelOption.TCP_NODELAY, true);

            final AtomicReference<TcpChannel> channel = new AtomicReference<TcpChannel>();
            b.handler(new ChannelInitializer<SocketChannel>() {

                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    channel.set(new TcpChannel(ch, remote));
                    ch.pipeline().addLast(channel.get(), new KryoMessageEncoder());
                }
            });

            b.connect(remote.getHost(), remote.getPort()).sync().awaitUninterruptibly();
            channel.get().ch.writeAndFlush(new InitiatorInfo(localEndpoint)).syncUninterruptibly();
            return channel.get();
        } catch (InterruptedException x) {
            x.printStackTrace();
            return null;
        }
    }

    public class TcpChannel extends ByteToMessageDecoder implements TransportConnection, RemoteEndpointUpdater {
        static final int ISIZE = Integer.SIZE / Byte.SIZE;

        SocketChannel ch;
        Endpoint remote;
        boolean failed = false;
        Throwable failureCause;

        TcpChannel(SocketChannel ch) {
            this.ch = ch;
        }

        TcpChannel(SocketChannel ch, Endpoint remote) {
            this.ch = ch;
            this.remote = remote;
            handler.onConnect(this);
        }

        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {

            if (in.readableBytes() > ISIZE) {
                int frameSize = (in.getInt(in.readerIndex()) & 0x07FFFFFF) + ISIZE;
                if (in.isReadable(frameSize)) {
                    Input inBuf = new Input(new ByteBufInputStream(in, frameSize));
                    inBuf.readInt();
                    Message pkt = (Message) KryoLib.kryo().readClassAndObject(inBuf);
                    if (pkt != null) {
                        pkt.setSize(frameSize);
                        try {
                            pkt.deliverTo(this, handler);
                        } catch (Exception x) {
                            x.printStackTrace();
                            System.exit(0);
                        }
                    }
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
            this.failed = true;
            handler.onFailure(this);
        }

        @Override
        public boolean failed() {
            return failed;
        }

        @Override
        public void dispose() {
            throw new RuntimeException("Not implemented...");
        }

        @Override
        public boolean send(Message m) {
            if (!failed) {
                ch.writeAndFlush(m).awaitUninterruptibly();
            }
            return !failed;
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
        public Throwable causeOfFailure() {
            return failureCause;
        }

        @Override
        public void setOption(String op, Object value) {
            throw new RuntimeException("Not implemented...");
        }

        @Override
        public void setRemoteEndpoint(Endpoint remote) {
            this.remote = remote;
            handler.onAccept(this);
        }
    }
}

class KryoMessageEncoder extends MessageToByteEncoder<Object> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Object obj, ByteBuf outBuf) {
        KryoLib.kryo().reset();
        outBuf.writeInt(-1);
        Output out = new Output(new ByteBufOutputStream(outBuf));
        KryoLib.kryo().writeClassAndObject(out, obj);
        out.close();
        outBuf.setInt(0, outBuf.writerIndex() - 4);
    }
}
