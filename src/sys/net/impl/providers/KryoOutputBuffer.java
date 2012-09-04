package sys.net.impl.providers;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import sys.net.impl.KryoLib;

import static sys.Sys.*;

import static sys.net.impl.NetworkingConstants.*;
import static sys.utils.Log.Log;

public class KryoOutputBuffer implements Runnable {

	final Kryo kryo = KryoLib.kryo();

	Output out;
	ByteBuffer buffer;

	Runnable handler;

	public KryoOutputBuffer() {
		buffer = ByteBuffer.allocate(KRYOBUFFER_INITIAL_CAPACITY);
		out = new Output(buffer.array(), Integer.MAX_VALUE);
	}

	public void run() {
	}


	final public int writeClassAndObjectFrame(Object object, WritableByteChannel ch) throws IOException {
		out.setPosition(4);
		kryo.writeClassAndObject(out, object);
		int length = out.position();

		if (length > buffer.capacity())
			buffer = ByteBuffer.wrap(out.getBuffer());
		
		buffer.clear();
		buffer.putInt(0, length - 4);
		buffer.limit(length);
		
		int n = ch.write(buffer);
		Sys.uploadedBytes.addAndGet(n);
		return n;
	}

	final public int writeClassAndObjectFrame(Object object) throws IOException {
		out.setPosition(4);
		kryo.writeClassAndObject(out, object);
		int length = out.position();

		if (length > buffer.capacity())
			buffer = ByteBuffer.wrap(out.getBuffer());

		buffer.clear();
		buffer.putInt(0, length - 4);
		buffer.limit(length);

		Sys.uploadedBytes.addAndGet( length );
		return length;
	}
	
	final public int writeClassAndObject(Object object) throws IOException {
		out.setPosition(0);
		kryo.writeClassAndObject(out, object);
		int length = out.position();

		if (length > buffer.capacity())
			buffer = ByteBuffer.wrap(out.getBuffer());
		
		out.flush();
		return length;
	}


	final public int writeClassAndObjectFrame(Object object, DataOutputStream dos) throws IOException {
		out.clear();
		kryo.writeClassAndObject(out, object);
		int length = out.position();

		if (length > buffer.capacity())
			buffer = ByteBuffer.wrap(out.getBuffer());

		dos.writeInt( length ) ;
		dos.write( buffer.array(), 0, length );	
		return length;
	}
	
	

	public ByteBuffer toByteBuffer() {
		return buffer;
	}
	
	public byte[] toByteArray() {
		byte[] res = new byte[buffer.limit()];
		System.arraycopy(buffer.array(), 0, res, 0, res.length);
		return res;
	}	
}