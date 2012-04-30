package swift.benchmarks.rpc;

import static sys.net.api.Networking.Networking;

import sys.net.impl.*;
import com.esotericsoftware.kryo.Kryo;

public class KryoSerialization {

	public static void init() {

		Kryo kryo = ((KryoSerializer) Networking.serializer()).kryo();
		kryo.setRegistrationOptional(true);

		kryo.register( Request.class ) ;
        kryo.register( Reply.class ) ;
		
	}
}
