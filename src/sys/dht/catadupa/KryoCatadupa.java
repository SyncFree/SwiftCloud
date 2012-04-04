package sys.dht.catadupa;

import static sys.net.api.Networking.Networking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import sys.dht.catadupa.crdts.ORSet;
import sys.dht.catadupa.crdts.time.LVV;
import sys.dht.catadupa.msgs.CatadupaCast;
import sys.dht.catadupa.msgs.CatadupaCastPayload;
import sys.dht.catadupa.msgs.DbMergeReply;
import sys.dht.catadupa.msgs.DbMergeRequest;
import sys.dht.catadupa.msgs.JoinRequest;
import sys.dht.catadupa.msgs.JoinRequestAccept;
import sys.net.impl.KryoSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serialize.CollectionSerializer;
import com.esotericsoftware.kryo.serialize.MapSerializer;

/**
 * Used to instruct the Kryo serializer about the classes used in the Catadupa
 * 1-hop DHT.
 * 
 * Manually registering classes will reduze the size of serialization and speed
 * things up (apparently...).
 * 
 * @author smd
 * 
 */
public class KryoCatadupa {

	public static void init() {

		Kryo kryo = ((KryoSerializer) Networking.serializer()).kryo();

		kryo.register(Map.class, new MapSerializer(kryo));
		kryo.register(HashMap.class, new MapSerializer(kryo));
		kryo.register(ArrayList.class, new CollectionSerializer(kryo));
		kryo.register(LinkedHashMap.class, new MapSerializer(kryo));

		kryo.register(Node.class);
		kryo.register(Node[].class);

		kryo.register(LVV.class);
		kryo.register(LVV.TS.class);
		kryo.register(LVV.TsSet.class);

		kryo.register(Range.class);

		kryo.register(JoinRequest.class);
		kryo.register(JoinRequestAccept.class);

		kryo.register(CatadupaCast.class);
		kryo.register(CatadupaCastPayload.class);
		kryo.register(DbMergeReply.class);
		kryo.register(DbMergeRequest.class);
		kryo.register(MembershipUpdate.class);
		kryo.register(ORSet.class);
		kryo.register(JoinRequest.class);

	}
}
