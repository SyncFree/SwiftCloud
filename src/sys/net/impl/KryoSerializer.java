package sys.net.impl;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import sys.net.api.Serializer;
import sys.net.api.SerializerException;

import com.esotericsoftware.kryo.Kryo;

public class KryoSerializer implements Serializer {
	private static int INITIAL_BUFFER_CAPACITY = 1 * 1024 * 1024;
	private KryoObjectBuffer outgoing; 
	private KryoObjectBuffer incoming;

	Kryo kryo;
	
	KryoSerializer() {
		kryo = new Kryo();
	}

	public Kryo getKryo() {
		return kryo;
	}

	@Override
	public byte[] writeObject(Object o) throws SerializerException {
			return outgoingBuffer().writeClassAndObject(o);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T readObject(byte[] data) throws SerializerException {
		return (T) incomingBuffer().readClassAndObject(data);
	}
	

	public void writeObject(DataOutputStream out, Object o) throws SerializerException {
		try {
			outgoingBuffer().writeClassAndObject(o, out);
		} catch (IOException e) {
			throw new SerializerException( e ) ;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T readObject(DataInputStream in) throws SerializerException {
		try {
			return (T) incomingBuffer().readClassAndObject(in);
		} catch (IOException e) {
			throw new SerializerException( e ) ;
		}
	}
	
	private KryoObjectBuffer outgoingBuffer() {
		if( outgoing == null ) {
			outgoing = new KryoObjectBuffer(kryo, INITIAL_BUFFER_CAPACITY, Integer.MAX_VALUE);
		}
		return outgoing;
	}
	
	private KryoObjectBuffer incomingBuffer() {
		if( incoming == null ) {
			incoming = new KryoObjectBuffer(kryo, INITIAL_BUFFER_CAPACITY, Integer.MAX_VALUE);
		}
		return incoming;
	}


}
