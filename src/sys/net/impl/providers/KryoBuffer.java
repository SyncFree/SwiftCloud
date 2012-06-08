package sys.net.impl.providers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.SerializationException;

import sys.net.impl.KryoLib;

import static sys.Sys.*;

public class KryoBuffer implements Runnable {

	final static Kryo kryo = KryoLib.kryo();

	private static int INITIAL_BUFFER_CAPACITY = 1 * 1024;

	ByteBuffer buffer;

	Runnable handler;

	public KryoBuffer() {
		buffer = ByteBuffer.allocate(INITIAL_BUFFER_CAPACITY);
	}

	public boolean readFrom(SocketChannel ch) throws IOException {
		buffer.clear().limit(4);

		while (ch.read(buffer) > 0 && buffer.hasRemaining())
			;

		if (buffer.hasRemaining())
			return false;

		int contentLength = buffer.getInt(0);

		Sys.downloadedBytes.addAndGet(4 + contentLength);

		if (contentLength > buffer.capacity()) {
			int newCapacity = Integer.highestOneBit(contentLength) << 2;
			buffer = ByteBuffer.allocate(newCapacity);
		}
		buffer.clear().limit(contentLength);

		while (ch.read(buffer) > 0 && buffer.hasRemaining())
			;

		if (buffer.hasRemaining())
			return false;

		buffer.flip();
		return true;
	}

	public void setHandler(Runnable handler) {
		this.handler = handler;
	}

	public void run() {
		handler.run();
	}

	public int writeClassAndObject(Object object) throws IOException {
		while (true) {
			buffer.clear();
			try {
				kryo.writeClassAndObject(buffer, object);
				break;
			} catch (SerializationException ex) { // For some reason, Kryo
													// throws this exception,
													// instead of the one
													// below...
				if (!resizeBuffer())
					throw ex;
			} catch (BufferOverflowException ex) {
				if (!resizeBuffer())
					throw ex;
			}
		}
		int res = buffer.position();
		buffer.flip();
		return res;
	}

	public void writeClassAndObjectFrame(Object object, SocketChannel ch) throws IOException {
		while (true) {
			buffer.clear();
			buffer.position(4);
			try {
				kryo.writeClassAndObject(buffer, object);
				break;
			} catch (SerializationException ex) { // For some reason, Kryo
													// throws this exception,
													// instead of the one
													// below...
				if (!resizeBuffer())
					throw ex;
			} catch (BufferOverflowException ex) {
				if (!resizeBuffer())
					throw ex;
			}
		}
		int contentLength = buffer.position();
		buffer.putInt(0, contentLength - 4);
		Sys.uploadedBytes.addAndGet(buffer.position());
		buffer.flip();
		ch.write(buffer);
	}

	public int writeClassAndObjectFrame(Object object) throws IOException {
		while (true) {
			buffer.clear();
			buffer.position(4);
			try {
				kryo.writeClassAndObject(buffer, object);
				break;
			} catch (SerializationException ex) { // For some reason, Kryo
													// throws this exception,
													// instead of the one
													// below...
				if (!resizeBuffer())
					throw ex;
			} catch (BufferOverflowException ex) {
				if (!resizeBuffer())
					throw ex;
			}
		}
		int contentLength = buffer.position();
		buffer.putInt(0, contentLength - 4);
		int length = buffer.position();
		Sys.uploadedBytes.addAndGet(length);
		buffer.flip();
		return length;
	}

	@SuppressWarnings("unchecked")
	static public <T> T readClassAndObject(ByteBuffer buffer) {
		return (T) kryo.readClassAndObject(buffer);
	}

	@SuppressWarnings("unchecked")
	public <T> T readClassAndObject() {
		return (T) kryo.readClassAndObject(buffer);
	}

	private boolean resizeBuffer() {
		buffer = ByteBuffer.allocate(buffer.capacity() * 2);
		return true;
	}

	public byte[] toByteArray() {
		byte[] objectBytes = new byte[buffer.limit()];
		System.arraycopy(buffer.array(), 0, objectBytes, 0, objectBytes.length);
		return objectBytes;
	}

	public ByteBuffer toByteBuffer() {
		return buffer;
	}

	public void writeTo(OutputStream os) throws IOException {
		os.write(buffer.array(), 0, buffer.position());
	}

	public void readFrom(InputStream is, int contentLength) throws IOException {
		if (contentLength > buffer.capacity()) {
			int newCapacity = Integer.highestOneBit(contentLength) << 2;
			buffer = ByteBuffer.allocate(newCapacity);
		}
		for (int n = 0; n < contentLength;)
			n += is.read(buffer.array(), n, contentLength - n);
	}
}
