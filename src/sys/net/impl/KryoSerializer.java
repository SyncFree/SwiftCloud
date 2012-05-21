package sys.net.impl;

import static sys.utils.Log.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import sys.net.api.Serializer;
import sys.net.api.SerializerException;
import sys.net.impl.providers.KryoBuffer;

import com.esotericsoftware.kryo.Kryo;

public class KryoSerializer implements Serializer {

    private KryoBuffer outgoing;
    private KryoBuffer incoming;

    KryoSerializer() {
        incoming = new KryoBuffer();
        outgoing = new KryoBuffer();
    }

    @Override
    public byte[] writeObject(Object o) throws SerializerException {
        synchronized (outgoing) {
            try {
                outgoing.writeClassAndObject(o);
                return outgoing.toByteArray();
            } catch (IOException e) {
                throw new SerializerException(e.getMessage());
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T readObject(byte[] data) throws SerializerException {
        return (T) KryoLib.kryo().readClassAndObject(ByteBuffer.wrap(data));
    }

    @Override
    public void writeObject(DataOutputStream out, Object o) throws SerializerException {
        try {
            synchronized (outgoing) {
                out.writeInt( outgoing.writeClassAndObject(o) ) ;
                outgoing.writeTo( out ) ;
            }
        } catch (IOException e) {
            Log.fine(String.format("Kryo Serialization Exception: ", e.getMessage()));
            throw new SerializerException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T readObject(DataInputStream in) throws SerializerException {
        try {
            synchronized (incoming) {
                incoming.readFrom( in, in.readInt());
                return (T) incoming.readClassAndObject();
            }
        } catch (IOException e) {
            Log.fine(String.format("Kryo Serialization Exception: ", e.getMessage()));
            throw new SerializerException(e);
        }
    }

}
