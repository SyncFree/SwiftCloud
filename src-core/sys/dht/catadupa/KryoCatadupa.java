/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package sys.dht.catadupa;

import sys.dht.catadupa.crdts.ORSet;
import sys.dht.catadupa.crdts.time.LVV;
import sys.dht.catadupa.msgs.CatadupaCast;
import sys.dht.catadupa.msgs.CatadupaCastPayload;
import sys.dht.catadupa.msgs.DbMergeReply;
import sys.dht.catadupa.msgs.DbMergeRequest;
import sys.dht.catadupa.msgs.JoinRequest;
import sys.dht.catadupa.msgs.JoinRequestAccept;
import sys.net.impl.KryoLib;

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
        KryoLib.register(Node.class, 0x30);
        KryoLib.register(Node[].class, 0x31);
        KryoLib.register(LVV.class, 0x32);
        KryoLib.register(LVV.TS.class, 0x33);
        KryoLib.register(LVV.TsSet.class, 0x34);
        KryoLib.register(Range.class, 0x35);
        KryoLib.register(JoinRequest.class, 0x36);
        KryoLib.register(JoinRequestAccept.class, 0x37);
        KryoLib.register(CatadupaCast.class, 0x38);
        KryoLib.register(CatadupaCastPayload.class, 0x39);
        KryoLib.register(DbMergeReply.class, 0x3A);
        KryoLib.register(DbMergeRequest.class, 0x3B);
        KryoLib.register(MembershipUpdate.class, 0x3C);
        KryoLib.register(ORSet.class, 0x3D);
        KryoLib.register(JoinRequest.class, 0x3E);
    }
}
