package sys.net.impl;

import static sys.net.api.Networking.Networking;

import java.nio.ByteBuffer;

import sys.dht.api.StringKey;
import sys.dht.impl.msgs.DHT_Request;
import sys.dht.impl.msgs.DHT_RequestReply;
import sys.net.impl.rpc.RpcPacket;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serialize.SimpleSerializer;

public class KryoSerialization {

	public static void init() {

		Kryo kryo = ((KryoSerializer) Networking.serializer()).kryo();
		kryo.setRegistrationOptional(true);

		kryo.register(LocalEndpoint.class, new SimpleSerializer<AbstractEndpoint>() {
			@Override
			public void write(ByteBuffer buffer, AbstractEndpoint ep) {
				buffer.putLong((Long) ep.locator());
			}

			@Override
			public AbstractEndpoint read(ByteBuffer buffer) {
				return new RemoteEndpoint(buffer.getLong());
			}
		});

		kryo.register(RemoteEndpoint.class, new SimpleSerializer<RemoteEndpoint>() {

			@Override
			public RemoteEndpoint read(ByteBuffer bb) {
				return new RemoteEndpoint(bb.getLong());
			}

			@Override
			public void write(ByteBuffer bb, RemoteEndpoint e) {
				bb.putLong((Long) e.locator());
			}
		});

		kryo.register(RpcPacket.class);
		kryo.register(DHT_Request.class);
		kryo.register(DHT_RequestReply.class);
		kryo.register(StringKey.class);
	}
}
