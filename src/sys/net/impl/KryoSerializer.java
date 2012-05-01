package sys.net.impl;

import static sys.utils.Log.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import sys.net.api.Serializer;
import sys.net.api.SerializerException;

import com.esotericsoftware.kryo.Kryo;

public class KryoSerializer implements Serializer {
    private static int INITIAL_BUFFER_CAPACITY = 1 * 1024;

    private KryoObjectBuffer outgoing;
    private KryoObjectBuffer incoming;


    KryoSerializer() {
        incoming = new KryoObjectBuffer(kryo(), INITIAL_BUFFER_CAPACITY, Integer.MAX_VALUE);
        outgoing = new KryoObjectBuffer(kryo(), INITIAL_BUFFER_CAPACITY, Integer.MAX_VALUE);
    }

    synchronized public Kryo kryo() {
        if( kryo == null ) {
            kryo = new Kryo();
        }
        return kryo;
    }

    @Override
    public byte[] writeObject(Object o) throws SerializerException {
        synchronized (outgoing) {
            return outgoing.writeClassAndObject(o);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T readObject(byte[] data) throws SerializerException {
        synchronized( incoming ) {
            return (T) incoming.readClassAndObject(data);
        }
    }

    @Override
    public void writeObject(DataOutputStream out, Object o) throws SerializerException {
        try {
            synchronized (outgoing) {
                outgoing.writeClassAndObject(o, out);
            }
        } catch (IOException e) {
            Log.fine( String.format("Kryo Serialization Exception: ", e.getMessage() ));
            throw new SerializerException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T readObject(DataInputStream in) throws SerializerException {
        try {
            synchronized( incoming ) {
                return (T) incoming.readClassAndObject(in);
            }
        } catch (IOException e) {
            Log.fine( String.format("Kryo Serialization Exception: ", e.getMessage() ));
            throw new SerializerException(e);
        }
    }

   
    private static Kryo kryo;
}
