package sys.net.impl;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import sys.net.api.Endpoint;
import sys.net.api.MessageHandler;

abstract public class AbstractEndpoint implements Endpoint {

    protected long gid;
    protected long locator;
    protected InetSocketAddress sockAddress;

    protected AtomicLong incomingBytesCounter = new AtomicLong(0);
    protected AtomicLong outgoingBytesCounter = new AtomicLong(0);

    protected MessageHandler handler;

    protected AbstractEndpoint() {
        this.handler = new DefaultMessageHandler(false);
        this.incomingBytesCounter = new AtomicLong(0);
        this.outgoingBytesCounter = new AtomicLong(0);
    }

    protected AbstractEndpoint(MessageHandler handler) {
        this.handler = handler;
        this.incomingBytesCounter = new AtomicLong(0);
        this.outgoingBytesCounter = new AtomicLong(0);
    }

    protected AbstractEndpoint(InetSocketAddress sockAddress, long gid) {
        this.gid = gid;
        this.sockAddress = sockAddress;
        this.locator = encodeLocator(sockAddress);
        this.incomingBytesCounter = new AtomicLong(0);
        this.outgoingBytesCounter = new AtomicLong(0);
    }

    protected AbstractEndpoint(long locator, long gid) {
        this.gid = gid;
        this.locator = locator;
        this.sockAddress = decodeLocator(locator);
        this.incomingBytesCounter = new AtomicLong(0);
        this.outgoingBytesCounter = new AtomicLong(0);
    }

    public final boolean isOutgoing() {
        return (locator & 0xFFFF) == 0;
    }

    public final boolean isIncoming() {
        return (locator & 0xFFFF) > 0;
    }

    @Override
    public int hashCode() {
        return (int) (locator >>> 32 ^ locator & 0xFFFFFFFFL);
    }

    final public boolean equals(AbstractEndpoint other) {
        return locator == other.locator && gid == other.gid;
    }

    @Override
    public boolean equals(Object other) {
        return other != null && equals((AbstractEndpoint) other);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Endpoint> T setHandler(MessageHandler handler) {
        this.handler = handler;
        return (T) this;
    }

    static protected void copyLocatorData(AbstractEndpoint src, AbstractEndpoint dst) {
        dst.gid = src.gid;
        dst.locator = src.locator;
        dst.sockAddress = src.sockAddress;
    }

    protected void setSocketAddress(int port) {
        try {
            this.sockAddress = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), port);
            this.locator = encodeLocator(sockAddress);

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public InetSocketAddress sockAddress() {
        return sockAddress;
    }

    @Override
    public MessageHandler getHandler() {
        return handler;
    }

    @Override
    public String toString() {
        if (sockAddress != null)
            return sockAddress.getAddress().getHostAddress() + ":" + sockAddress.getPort()
                    + (gid == 0L ? "" : "/" + Long.toString(gid, 32));
        else
            return "?????????????????????";
    }

    @SuppressWarnings("unchecked")
    public <T> T gid() {
        return (T) new Long(gid);
    }

    protected static long encodeLocator(InetSocketAddress addr) {
        return ((long) ByteBuffer.wrap(addr.getAddress().getAddress()).getInt() << Integer.SIZE) | addr.getPort();
    }

    protected static InetSocketAddress decodeLocator(long locator) {
        try {
            ByteBuffer buf = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt((int) (locator >> Integer.SIZE));
            return new InetSocketAddress(InetAddress.getByAddress(buf.array()), (int) (locator & 0x0FFFF));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    final public void write(Kryo kryo, Output output) {
        output.writeLong(this.locator);
        output.writeLong(this.gid);
    }

    final public void read(Kryo kryo, Input input) {
        this.locator = input.readLong();
        this.gid = input.readLong();
    }

    public String getHost() {
        return sockAddress().getHostName();
    }

    public int getPort() {
        return sockAddress().getPort();
    }

    public AtomicLong getIncomingBytesCounter() {
        if (incomingBytesCounter == null)
            return (incomingBytesCounter = new AtomicLong(0));
        return incomingBytesCounter;
    }

    public int getConnectionRetryDelay() {
        return 0;
    }
    
    public AtomicLong getOutgoingBytesCounter() {
        if (outgoingBytesCounter == null)
            return (outgoingBytesCounter = new AtomicLong(0));
        return outgoingBytesCounter;
    }
}