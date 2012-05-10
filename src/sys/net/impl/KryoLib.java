package sys.net.impl;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serialize.SimpleSerializer;

import sys.dht.api.StringKey;
import sys.dht.impl.msgs.DHT_Request;
import sys.dht.impl.msgs.DHT_RequestReply;

public class KryoLib {

	public static <T> void register(Class<T> cl) {
		if (kryo == null)
			registry.put(cl, null);
		else
			kryo.register(cl);
	}

	public static void register(Class<?> cl, Serializer serializer) {
		if (kryo == null)
			registry.put(cl, serializer);
		else
			kryo.register(cl, serializer);
	}

	synchronized public static Kryo kryo() {
		if (kryo == null) {
			kryo = new Kryo();
			for (Map.Entry<Class<?>, Serializer> i : registry.entrySet()) {
				if (i.getValue() == null)
					kryo.register(i.getClass());
				else
					kryo.register(i.getKey(), i.getValue());
			}
			kryo.setRegistrationOptional(true);
		}
		return kryo;
	}

	public static void init() {

		register(LocalEndpoint.class, new SimpleSerializer<AbstractEndpoint>() {
			@Override
			public void write(ByteBuffer buffer, AbstractEndpoint ep) {
				buffer.putLong((Long) ep.locator());
			}

			@Override
			public AbstractEndpoint read(ByteBuffer buffer) {
				return new RemoteEndpoint(buffer.getLong());
			}
		});

		register(RemoteEndpoint.class, new SimpleSerializer<RemoteEndpoint>() {

			@Override
			public RemoteEndpoint read(ByteBuffer bb) {
				return new RemoteEndpoint(bb.getLong());
			}

			@Override
			public void write(ByteBuffer bb, RemoteEndpoint e) {
				bb.putLong((Long) e.locator());
			}
		});

		register(DHT_Request.class);
		register(DHT_RequestReply.class);
		register(StringKey.class);
	}

	public static Kryo kryo = null;
	private static Map<Class<?>, Serializer> registry = new LinkedHashMap<Class<?>, Serializer>();
}