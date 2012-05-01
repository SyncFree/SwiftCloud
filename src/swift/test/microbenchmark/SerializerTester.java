package swift.test.microbenchmark;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.http.util.ByteArrayBuffer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serialize.CollectionSerializer;
import com.esotericsoftware.kryo.serialize.MapSerializer;

import swift.clocks.VersionVectorWithExceptions;
import sys.net.impl.KryoSerialization;
import sys.net.impl.KryoSerializer;

public class SerializerTester {

    /**
     * @param args
     */
    public static void main(String[] args) {
        Kryo kryo = new Kryo();
        VersionVectorWithExceptions vv = new VersionVectorWithExceptions();

        kryo.register(Map.class, new MapSerializer(kryo));
        kryo.register(HashMap.class, new MapSerializer(kryo));
        kryo.register(ArrayList.class, new CollectionSerializer(kryo));
        kryo.register(LinkedHashMap.class, new MapSerializer(kryo));
        kryo.register(VersionVectorWithExceptions.class);
        
        ByteBuffer bb = ByteBuffer.allocate(65536);
        kryo.writeObject(bb, "teste");

    }

}
