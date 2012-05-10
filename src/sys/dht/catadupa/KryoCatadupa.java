package sys.dht.catadupa;

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
import sys.net.impl.KryoLib;

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


//		kryo.register(Map.class, new MapSerializer(kryo));
//		kryo.register(HashMap.class, new MapSerializer(kryo));
//		kryo.register(ArrayList.class, new CollectionSerializer(kryo));
//		kryo.register(LinkedHashMap.class, new MapSerializer(kryo));

		KryoLib.register(Node.class);
		KryoLib.register(Node[].class);

		KryoLib.register(LVV.class);
		KryoLib.register(LVV.TS.class);
		KryoLib.register(LVV.TsSet.class);

		KryoLib.register(Range.class);

		KryoLib.register(JoinRequest.class);
		KryoLib.register(JoinRequestAccept.class);

		KryoLib.register(CatadupaCast.class);
		KryoLib.register(CatadupaCastPayload.class);
		KryoLib.register(DbMergeReply.class);
		KryoLib.register(DbMergeRequest.class);
		KryoLib.register(MembershipUpdate.class);
		KryoLib.register(ORSet.class);
		KryoLib.register(JoinRequest.class);

	}
}
