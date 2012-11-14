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
package sys.net.impl.providers;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import sys.net.api.Endpoint;
import sys.net.impl.KryoLib;

import static sys.Sys.*;

import static sys.net.impl.NetworkingConstants.*;

public class KryoInputBuffer implements Runnable {

	private static Logger Log = Logger.getLogger( KryoInputBuffer.class.getName() );


	final Kryo kryo = KryoLib.kryo();

	Input in;
	ByteBuffer buffer;
	protected int contentLength;

	Runnable handler;

	public KryoInputBuffer() {
		buffer = ByteBuffer.allocate(KRYOBUFFER_INITIAL_CAPACITY);
		in = new Input(buffer.array());
	}

	public void run() {
		handler.run();
	}

	final public void setHandler(Runnable handler) {
		this.handler = handler;
	}

	final public boolean readFrom(ReadableByteChannel ch) throws IOException {
		int c = 1;

		buffer.clear().limit(4);
		while (buffer.hasRemaining() && (c = ch.read(buffer)) > 0);

		if (buffer.hasRemaining()) {
			Log.finest("#####ERROR: READING MSG HEADER:" + c);
			return false;
		}

		contentLength = buffer.getInt(0);

		ensureCapacity(contentLength);

		buffer.clear().limit(contentLength);
		while (buffer.hasRemaining() && (c = ch.read(buffer)) > 0);
		    
		if (buffer.hasRemaining()) {
			Log.finest("#####ERROR: READING MSG BODY:" + c);
			return false;
		}

		buffer.flip();
		contentLength += 4;
		Sys.downloadedBytes.addAndGet(contentLength);
		return true;
	}

	final public boolean readFrom(DataInputStream in) throws IOException {

		contentLength = in.readInt();

		ensureCapacity(contentLength);

		in.readFully(buffer.array());

		contentLength += 4;
		Sys.downloadedBytes.addAndGet(contentLength);
		return true;
	}

	public ByteBuffer toByteBuffer() {
		return buffer;
	}

	public byte[] toByteArray() {
		byte[] res = new byte[buffer.limit()];
		System.arraycopy(buffer.array(), 0, res, 0, res.length);
		return res;
	}

	@SuppressWarnings("unchecked")
	public <T> T readClassAndObject() {
		try {
			in.setPosition(0);
			return (T) kryo.readClassAndObject(in);
		} catch (Exception x) {
			throw new RuntimeException( x.getMessage() );
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T readClassAndObject(ByteBuffer buf) {
		return (T) kryo.readClassAndObject(new Input(buf.array()));
	}

	final private void ensureCapacity(int required) {
		if (required > buffer.array().length) {
			buffer = ByteBuffer.allocate(nextPowerOfTwo(required));
			in.setBuffer(buffer.array());
		}
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