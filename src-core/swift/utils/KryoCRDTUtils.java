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

import swift.client.proto.ClientRequest;
import swift.client.proto.CommitUpdatesReply;
import swift.client.proto.CommitUpdatesRequest;
import swift.client.proto.FastRecentUpdatesReply;
import swift.client.proto.FastRecentUpdatesRequest;
import swift.client.proto.FetchObjectDeltaRequest;
import swift.client.proto.FetchObjectVersionReply;
import swift.client.proto.FetchObjectVersionRequest;
import swift.client.proto.GenerateTimestampReply;
import swift.client.proto.GenerateTimestampRequest;
import swift.client.proto.KeepaliveReply;
import swift.client.proto.KeepaliveRequest;
import swift.client.proto.LatestKnownClockReply;
import swift.client.proto.LatestKnownClockRequest;
import swift.client.proto.RecentUpdatesReply;
import swift.client.proto.RecentUpdatesRequest;
import swift.client.proto.SubscriptionType;
import swift.client.proto.UnsubscribeUpdatesRequest;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.clocks.TripleTimestamp;
import swift.clocks.VersionVectorWithExceptions;
import swift.crdt.BaseCRDT;
import swift.crdt.CRDTIdentifier;
import swift.crdt.IntegerVersioned;
import swift.crdt.RegisterVersioned;
import swift.crdt.SetIds;
import swift.crdt.SetIntegers;
import swift.crdt.SetMsg;
import swift.crdt.SetStrings;
import swift.crdt.SetVersioned;
import swift.crdt.operations.BaseUpdate;
import swift.crdt.operations.CRDTObjectUpdatesGroup;
import swift.crdt.operations.IntegerUpdate;
import swift.crdt.operations.RegisterUpdate;
import swift.crdt.operations.SetInsert;
import swift.crdt.operations.SetRemove;
import swift.dc.proto.CommitTSReply;
import swift.dc.proto.CommitTSRequest;
import swift.dc.proto.DHTExecCRDT;
import swift.dc.proto.DHTExecCRDTReply;
import swift.dc.proto.DHTGetCRDT;
import swift.dc.proto.DHTGetCRDTReply;
import swift.dc.proto.DHTSendNotification;
import sys.net.impl.KryoLib;

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
        registerable.register(BaseCRDT.class, 0x56);

        registerable.register(RegisterVersioned.class, 0x58);
        registerable.register(RegisterVersioned.UpdateEntry.class, 0x59);
        registerable.register(SetIds.class, 0x60);
        registerable.register(SetIntegers.class, 0x61);
        registerable.register(SetMsg.class, 0x62);
        registerable.register(SetStrings.class, 0x63);
        registerable.register(SetVersioned.class, 0x64);
        // registerable.register(LogootVersioned.class, 0x65);
        // registerable.register(LogootDocument.class, 0x66);
        // registerable.register(LogootDocumentWithTombstones.class, 0x67);
        // registerable.register(Component.class, 0x68);
        // registerable.register(LogootIdentifier.class, 0x69);
        // registerable.register(RangeList.class, 0x6A);
        // registerable.register(LogootInsert.class, 0x6B);
        // registerable.register(LogootDelete.class, 0x6C);
        registerable.register(IntegerVersioned.class, 0x6D);

        registerable.register(ClientRequest.class, 0x70);
        registerable.register(CommitUpdatesRequest.class, 0x71);
        registerable.register(CommitUpdatesReply.class, 0x72);
        registerable.register(CommitUpdatesReply.CommitStatus.class, 0x73);
        registerable.register(FastRecentUpdatesRequest.class, 0x74);
        registerable.register(FastRecentUpdatesReply.class, 0x75);
        registerable.register(FastRecentUpdatesReply.ObjectSubscriptionInfo.class, 0x76);
        registerable.register(FetchObjectDeltaRequest.class, 0x77);
        registerable.register(FetchObjectVersionRequest.class, 0x78);
        registerable.register(FetchObjectVersionReply.class, 0x79);
        registerable.register(FetchObjectVersionReply.FetchStatus.class, 0x7A);
        registerable.register(GenerateTimestampRequest.class, 0x7B);
        registerable.register(GenerateTimestampReply.class, 0x7C);
        registerable.register(KeepaliveRequest.class, 0x7D);
        registerable.register(KeepaliveReply.class, 0x7E);
        registerable.register(LatestKnownClockRequest.class, 0x7F);
        registerable.register(LatestKnownClockReply.class, 0x80);
        registerable.register(RecentUpdatesRequest.class, 0x81);
        registerable.register(RecentUpdatesReply.class, 0x82);
        registerable.register(SubscriptionType.class, 0x83);
        registerable.register(UnsubscribeUpdatesRequest.class, 0x84);

        registerable.register(CRDTObjectUpdatesGroup.class, 0x85);
        registerable.register(BaseUpdate.class, 0x86);
        registerable.register(IntegerUpdate.class, 0x87);
        registerable.register(RegisterUpdate.class, 0x88);
        registerable.register(SetInsert.class, 0x89);
        registerable.register(SetRemove.class, 0x8A);

        registerable.register(CommitTSRequest.class, 0x8B);
        registerable.register(CommitTSReply.class, 0x8C);
        registerable.register(DHTExecCRDT.class, 0x8D);
        registerable.register(DHTExecCRDTReply.class, 0x8E);
        registerable.register(DHTGetCRDT.class, 0x8F);
        registerable.register(DHTGetCRDTReply.class, 0x90);
        registerable.register(DHTSendNotification.class, 0x91);

        registerable.register(ArrayList.class, 0xA0);
        registerable.register(LinkedList.class, 0xA1);
        registerable.register(TreeMap.class, 0xA2);
        registerable.register(HashMap.class, 0xA3);
        registerable.register(Map.Entry.class, 0xA4);
        registerable.register(TreeSet.class, 0xA5);

        // WISHME: I tried to use Reflections library, but it has plenty of
        // dependencies...
        // final Reflections reflectionsLib = new Reflections("swift");
        // final Set<Class<?>> classes =
        // reflectionsLib.getTypesAnnotatedWith(RegisterInKryo.class);
        // // We need to register them in the very same order at every JVM.
        // final ArrayList<Class<?>> classesSorted = new
        // ArrayList<Class<?>>(classes);
        // Collections.sort(classesSorted, new Comparator<Class<?>>() {
        // @Override
        // public int compare(Class<?> o1, Class<?> o2) {
        // return o1.getCanonicalName().compareTo(o2.getCanonicalName());
        // }
        // });
        //
        // for (final Class<?> cls : classesSorted) {
        // kryo.register(cls);
        // }
    }
}
