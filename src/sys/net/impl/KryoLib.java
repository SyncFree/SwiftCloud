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
import sys.net.api.CustomKryoSerializer;
import sys.net.impl.providers.LocalEndpointExchange;
import sys.net.impl.providers.nio.TcpEndpoint;

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
				buffer.putLong(ep.locator);
				buffer.putLong(ep.gid);
			}

			@Override
			public AbstractEndpoint read(ByteBuffer buffer) {
				return new RemoteEndpoint(buffer.getLong(), buffer.getLong());
			}
		});

		register(RemoteEndpoint.class, new SimpleSerializer<AbstractEndpoint>() {

			@Override
			public void write(ByteBuffer buffer, AbstractEndpoint ep) {
				buffer.putLong(ep.locator);
				buffer.putLong(ep.gid);
			}

			@Override
			public AbstractEndpoint read(ByteBuffer buffer) {
				return new RemoteEndpoint(buffer.getLong(), buffer.getLong());
			}
		});
		register(LocalEndpointExchange.class);

		register(DHT_Request.class);
		register(DHT_RequestReply.class);
		register(StringKey.class);
	}

	// public static void autoRegister( String packagePrefix ) {
	// for( Package p : Package.getPackages() ) {
	// System.out.println( p.getName() );
	// if( p.getName().startsWith( packagePrefix ) )
	// for( Class<?> c : p.getClass().getClasses() ) ;
	// }
	// }
	//
	// private static void processClasse( Class<?> c ) {
	// if(c.isInstance( CustomKryoSerializer.class) ) {
	// System.out.println(c);
	// }
	// }

	public static Kryo kryo = null;
	private static Map<Class<?>, Serializer> registry = new LinkedHashMap<Class<?>, Serializer>();
}