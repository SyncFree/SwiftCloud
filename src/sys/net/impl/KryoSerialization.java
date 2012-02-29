package sys.net.impl;

import java.nio.ByteBuffer;

import sys.dht.api.StringKey;
import sys.dht.impl.msgs.DHT_RequestReply;
import sys.dht.impl.msgs.DHT_Request;
import sys.net.impl.rpc.RpcPayload;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serialize.SimpleSerializer;

import static sys.net.api.Networking.*;

public class KryoSerialization {

	public static void init() {
		
		Kryo kryo = ((KryoSerializer)Networking.serializer()).getKryo();
		kryo.setRegistrationOptional(true);

		kryo.register(LocalEndpoint.class, new SimpleSerializer<AbstractEndpoint>() {
			public void write(ByteBuffer buffer, AbstractEndpoint ep) {
				buffer.putLong( (Long)ep.locator());
			}

			public AbstractEndpoint read(ByteBuffer buffer) {
				return new RemoteEndpoint(buffer.getLong());
			}
		});
		
		kryo.register(RemoteEndpoint.class, new SimpleSerializer<RemoteEndpoint>() {

			@Override
			public RemoteEndpoint read(ByteBuffer bb) {
				return new RemoteEndpoint( bb.getLong() );
			}

			@Override
			public void write(ByteBuffer bb, RemoteEndpoint e) {
				bb.putLong( (Long)e.locator() ) ;
			}
		});

		
		kryo.register(RpcPayload.class);
		kryo.register(DHT_Request.class);
        kryo.register(DHT_RequestReply.class);
        kryo.register(StringKey.class);
	}
}
