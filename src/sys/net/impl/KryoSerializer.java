package sys.net.impl;

import static sys.utils.Log.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import sys.net.api.Serializer;
import sys.net.api.SerializerException;
import sys.net.impl.providers.KryoInputBuffer;
import sys.net.impl.providers.KryoOutputBuffer;

public class KryoSerializer implements Serializer {

	private KryoInputBuffer input;
	private KryoOutputBuffer output;

	public KryoSerializer() {
		input = new KryoInputBuffer();
		output = new KryoOutputBuffer();
	}

	@Override
	synchronized public byte[] writeObject(Object o) throws SerializerException {
		try {
			output.writeClassAndObject(o);
			return output.toByteArray();
		} catch (IOException e) {
			throw new SerializerException(e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	synchronized public <T> T readObject(byte[] data) throws SerializerException {
		return (T) input.readClassAndObject(ByteBuffer.wrap(data));
	}

	@Override
	synchronized public void writeObject(DataOutputStream out, Object o) throws SerializerException {
		try {
			output.writeClassAndObjectFrame(o, out);
		} catch (IOException e) {
			Log.fine(String.format("Kryo Serialization Exception: ", e.getMessage()));
			throw new SerializerException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	synchronized public <T> T readObject(DataInputStream in) throws SerializerException {
		try {
			input.readFrom(in);
			return (T) input.readClassAndObject();
		} catch (IOException e) {
			Log.fine(String.format("Kryo Serialization Exception: ", e.getMessage()));
			throw new SerializerException(e);
		}
	}
}
