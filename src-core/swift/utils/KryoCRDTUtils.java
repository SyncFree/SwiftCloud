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
package swift.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.clocks.TripleTimestamp;
import swift.clocks.VersionVectorWithExceptions;
import swift.crdt.AddOnlySetCRDT;
import swift.crdt.AddOnlySetUpdate;
import swift.crdt.AddWinsSetCRDT;
import swift.crdt.IntegerCRDT;
import swift.crdt.IntegerUpdate;
import swift.crdt.LWWRegisterCRDT;
import swift.crdt.LWWRegisterUpdate;
import swift.crdt.AddWinsSetAddUpdate;
import swift.crdt.AddWinsSetRemoveUpdate;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.ManagedCRDT;
import swift.proto.ClientRequest;
import swift.proto.CommitTSReply;
import swift.proto.CommitTSRequest;
import swift.proto.CommitUpdatesReply;
import swift.proto.CommitUpdatesRequest;
import swift.proto.DHTExecCRDT;
import swift.proto.DHTExecCRDTReply;
import swift.proto.DHTGetCRDT;
import swift.proto.DHTGetCRDTReply;
import swift.proto.FetchObjectDeltaRequest;
import swift.proto.FetchObjectVersionReply;
import swift.proto.FetchObjectVersionRequest;
import swift.proto.LatestKnownClockReply;
import swift.proto.LatestKnownClockRequest;
import swift.proto.UnsubscribeUpdatesRequest;
import sys.net.impl.KryoLib;
//import swift.dc.proto.DHTSendNotification;
//import swift.client.proto.FastRecentUpdatesReply;
//import swift.client.proto.FastRecentUpdatesRequest;

/**
 * Registers serializable SwiftCloud classes for static mapping in Kryo.
 * 
 * @author mzawirski
 */
public class KryoCRDTUtils {
    public interface Registerable {
        void register(Class<?> cl, int id);
    }

    /**
     * Needs to be called exactly only once per JVM!
     */
    public static void init() {
        registerCRDTClasses(new Registerable() {
            @Override
            public void register(Class<?> cl, int id) {
                KryoLib.register(cl, id);
            }
        });
    }

    public static void registerCRDTClasses(Registerable registerable) {

        // FIXME: remove dependency from src-core to src-app!

        registerable.register(Timestamp.class, 0x50);
        registerable.register(TripleTimestamp.class, 0x51);
        registerable.register(VersionVectorWithExceptions.class, 0x52);
        registerable.register(VersionVectorWithExceptions.Interval.class, 0x53);
        registerable.register(TimestampMapping.class, 0x54);
        registerable.register(CRDTIdentifier.class, 0x55);
        registerable.register(ManagedCRDT.class, 0x56);

        registerable.register(LWWRegisterCRDT.class, 0x58);
        // 0x59
        // registerable.register(SetIds.class, 0x60);
        // registerable.register(SetIntegers.class, 0x61);
        // registerable.register(SetMsg.class, 0x62);
        // registerable.register(SetStrings.class, 0x63);
        // registerable.register(SetVersioned.class, 0x64);
        registerable.register(IntegerCRDT.class, 0x6D);

        registerable.register(ClientRequest.class, 0x70);
        registerable.register(CommitUpdatesRequest.class, 0x71);
        registerable.register(CommitUpdatesReply.class, 0x72);
        registerable.register(CommitUpdatesReply.CommitStatus.class, 0x73);

        registerable.register(FetchObjectDeltaRequest.class, 0x77);
        registerable.register(FetchObjectVersionRequest.class, 0x78);
        registerable.register(FetchObjectVersionReply.class, 0x79);
        registerable.register(FetchObjectVersionReply.FetchStatus.class, 0x7A);

        registerable.register(LatestKnownClockRequest.class, 0x7F);
        registerable.register(LatestKnownClockReply.class, 0x80);

        registerable.register(UnsubscribeUpdatesRequest.class, 0x81);

        registerable.register(CRDTObjectUpdatesGroup.class, 0x82);
        registerable.register(AddWinsSetCRDT.class, 0x83);
        registerable.register(IntegerUpdate.class, 0x84);
        registerable.register(LWWRegisterUpdate.class, 0x85);
        registerable.register(AddWinsSetAddUpdate.class, 0x86);
        registerable.register(AddWinsSetRemoveUpdate.class, 0x87);
        registerable.register(AddOnlySetCRDT.class, 0x88);
        registerable.register(AddOnlySetUpdate.class, 0x89);

        registerable.register(CommitTSRequest.class, 0x8B);
        registerable.register(CommitTSReply.class, 0x8C);
        registerable.register(DHTExecCRDT.class, 0x8D);
        registerable.register(DHTExecCRDTReply.class, 0x8E);
        registerable.register(DHTGetCRDT.class, 0x8F);
        registerable.register(DHTGetCRDTReply.class, 0x90);

        registerable.register(ArrayList.class, 0xA0);
        registerable.register(LinkedList.class, 0xA1);
        registerable.register(TreeMap.class, 0xA2);
        registerable.register(HashMap.class, 0xA3);
        registerable.register(Map.Entry.class, 0xA4);
        registerable.register(TreeSet.class, 0xA5);

    }
}
