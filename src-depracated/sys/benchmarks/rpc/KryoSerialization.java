package sys.benchmarks.rpc;

import sys.net.impl.*;
import com.esotericsoftware.kryo.Kryo;

public class KryoSerialization {

	public static void init() {

		Kryo kryo = KryoLib.kryo();
		kryo.setRegistrationOptional(true);

		
	}
}
