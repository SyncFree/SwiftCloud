package sys.net.impl.providers;

import static sys.net.impl.NetworkingConstants.KRYOBUFFER_INITIAL_CAPACITY;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

import swift.exceptions.NetworkException;
import sys.net.api.Message;
import sys.net.impl.KryoLib;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

final public class KryoBuffer {
    private static Logger Log = Logger.getLogger(KryoBuffer.class.getName());

    int uses;
    final Input in;
    final Output out;
    ByteBuffer buffer;

    Runnable handler;

    public KryoBuffer() {
        uses = 0;
        buffer = ByteBuffer.allocate(KRYOBUFFER_INITIAL_CAPACITY);
        out = new Output(buffer.array(), Integer.MAX_VALUE);
        in = new Input(buffer.array());
    }

    public int uses() {
        return uses;
    }

    public int writeClassAndObject(Object object, SocketChannel ch) throws IOException {
        uses++;
        out.setPosition(4);
        KryoLib.kryo().writeClassAndObject(out, object);
        int size = out.position();

        if (size > buffer.capacity()) {
            buffer = ByteBuffer.wrap(out.getBuffer());
            in.setBuffer(out.getBuffer());
        }

        buffer.clear();
        buffer.putInt(0, size - 4);
        buffer.limit(size);
        int n = ch.write(buffer);
        return n;
    }

    public int writeClassAndObject(Object object, OutputStream os) throws IOException {
        uses++;
        out.setPosition(4);
        KryoLib.kryo().writeClassAndObject(out, object);
        int size = out.position();

        if (size > buffer.capacity()) {
            buffer = ByteBuffer.wrap(out.getBuffer());
            in.setBuffer(out.getBuffer());
        }

        buffer.clear();
        buffer.putInt(0, size - 4);
        buffer.limit(size);
        os.write(buffer.array(), 0, size);
        return size;
    }

    final public Message readFrom(SocketChannel ch) throws NetworkException, IOException {
        int c = 1;

        buffer.clear().limit(4);
        while (buffer.hasRemaining() && (c = ch.read(buffer)) > 0)
            ;

        if (buffer.hasRemaining()) {
            throw new NetworkException("ERROR: Short Read");
        }

        int size = buffer.getInt(0);
        if (size > buffer.capacity()) {
            buffer = ByteBuffer.allocate(nextPowerOfTwo(size));
            in.setBuffer(buffer.array());
            out.setBuffer(buffer.array());
        }

        buffer.clear().limit(size);
        while (buffer.hasRemaining() && (c = ch.read(buffer)) > 0)
            ;

        if (buffer.hasRemaining()) {
            Log.finest("#####ERROR: READING MSG BODY:" + c);
            throw new NetworkException("ERROR: Short Read");
        }

        buffer.flip();
        in.rewind();
        Message msg = (Message) KryoLib.kryo().readClassAndObject(in);
        if (msg != null)
            msg.setSize(size + 4);

        // contentLength += 4;
        // byteCounter.addAndGet(contentLength);
        // Sys.downloadedBytes.addAndGet(contentLength);
        return msg;
    }

    static private int nextPowerOfTwo(int value) {
        if (value == 0)
            return 1;
        if ((value & value - 1) == 0)
            return value;
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        return value + 1;
    }
}