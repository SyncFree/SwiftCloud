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

import swift.application.social.Message;
import swift.application.social.User;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.clocks.TripleTimestamp;
import swift.clocks.VersionVectorWithExceptions;
import swift.crdt.AbstractAddOnlySetCRDT;
import swift.crdt.AbstractAddWinsSetCRDT;
import swift.crdt.AbstractLWWRegisterCRDT;
import swift.crdt.AbstractPutOnlyLWWMapCRDT;
import swift.crdt.AddOnlySetCRDT;
import swift.crdt.AddOnlySetUpdate;
import swift.crdt.AddOnlyStringSetCRDT;
import swift.crdt.AddOnlyStringSetUpdate;
import swift.crdt.AddWinsMessageSetCRDT;
import swift.crdt.AddWinsMessageSetUpdate;
import swift.crdt.AddWinsSetCRDT;
import swift.crdt.AddWinsSetUpdate;
import swift.crdt.IntegerCRDT;
import swift.crdt.IntegerUpdate;
import swift.crdt.LWWRegisterCRDT;
import swift.crdt.LWWRegisterUpdate;
import swift.crdt.LWWStringMapRegisterCRDT;
import swift.crdt.LWWStringMapRegisterUpdate;
import swift.crdt.LWWStringRegisterCRDT;
import swift.crdt.LWWStringRegisterUpdate;
import swift.crdt.LWWUserRegisterCRDT;
import swift.crdt.LWWUserRegisterUpdate;
import swift.crdt.PutOnlyLWWMapCRDT;
import swift.crdt.PutOnlyLWWMapUpdate;
import swift.crdt.PutOnlyLWWStringMapCRDT;
import swift.crdt.PutOnlyLWWStringMapUpdate;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.ManagedCRDT;
import swift.proto.BatchCommitUpdatesReply;
import swift.proto.BatchCommitUpdatesRequest;
import swift.proto.BatchFetchObjectVersionReply;
import swift.proto.BatchFetchObjectVersionRequest;
import swift.proto.ClientRequest;
import swift.proto.CommitTSReply;
import swift.proto.CommitTSRequest;
import swift.proto.CommitUpdatesReply;
import swift.proto.CommitUpdatesRequest;
import swift.proto.DHTExecCRDT;
import swift.proto.DHTExecCRDTReply;
import swift.proto.DHTGetCRDT;
import swift.proto.DHTGetCRDTReply;
import swift.proto.LatestKnownClockReply;
import swift.proto.LatestKnownClockRequest;
import swift.proto.UnsubscribeUpdatesRequest;
import swift.pubsub.BatchUpdatesNotification;
import swift.pubsub.UpdateNotification;
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
        int classId = 0x50;

        // Java primitives
        registerable.register(ArrayList.class, classId++);
        registerable.register(LinkedList.class, classId++);
        registerable.register(TreeMap.class, classId++);
        registerable.register(HashMap.class, classId++);
        registerable.register(Map.Entry.class, classId++);
        registerable.register(TreeSet.class, classId++);

        // Metadata
        registerable.register(Timestamp.class, classId++);
        registerable.register(TripleTimestamp.class, classId++);
        registerable.register(VersionVectorWithExceptions.class, classId++);
        registerable.register(VersionVectorWithExceptions.Interval.class, classId++);
        registerable.register(TimestampMapping.class, classId++);
        registerable.register(CRDTIdentifier.class, classId++);
        registerable.register(ManagedCRDT.class, classId++);
        registerable.register(CRDTObjectUpdatesGroup.class, classId++);

        // DC protocol
        registerable.register(CommitTSRequest.class, classId++);
        registerable.register(CommitTSReply.class, classId++);
        registerable.register(DHTExecCRDT.class, classId++);
        registerable.register(DHTExecCRDTReply.class, classId++);
        registerable.register(DHTGetCRDT.class, classId++);
        registerable.register(DHTGetCRDTReply.class, classId++);

        // Client-DC protocol
        registerable.register(ClientRequest.class, classId++);
        registerable.register(CommitUpdatesRequest.class, classId++);
        registerable.register(CommitUpdatesReply.class, classId++);
        registerable.register(CommitUpdatesReply.CommitStatus.class, classId++);
        registerable.register(BatchCommitUpdatesRequest.class, classId++);
        registerable.register(BatchCommitUpdatesReply.class, classId++);
        registerable.register(BatchUpdatesNotification.class, classId++);
        registerable.register(UpdateNotification.class, classId++);

        registerable.register(BatchFetchObjectVersionRequest.class, classId++);
        registerable.register(BatchFetchObjectVersionReply.class, classId++);
        registerable.register(BatchFetchObjectVersionReply.FetchStatus.class, classId++);

        registerable.register(LatestKnownClockRequest.class, classId++);
        registerable.register(LatestKnownClockReply.class, classId++);

        registerable.register(UnsubscribeUpdatesRequest.class, classId++);

        // SwiftSocial objects
        registerable.register(User.class, classId++);
        registerable.register(Message.class, classId++);

        // CRDTs
        registerable.register(IntegerCRDT.class, classId++);
        registerable.register(IntegerUpdate.class, classId++);

        registerable.register(AbstractLWWRegisterCRDT.class, classId++);
        registerable.register(LWWRegisterUpdate.class, classId++);
        registerable.register(LWWRegisterCRDT.class, classId++);
        registerable.register(LWWStringRegisterCRDT.class, classId++);
        registerable.register(LWWStringRegisterUpdate.class, classId++);
        registerable.register(LWWStringMapRegisterCRDT.class, classId++);
        registerable.register(LWWStringMapRegisterUpdate.class, classId++);
        registerable.register(LWWUserRegisterCRDT.class, classId++);
        registerable.register(LWWUserRegisterUpdate.class, classId++);

        registerable.register(AbstractPutOnlyLWWMapCRDT.class, classId++);
        registerable.register(AbstractPutOnlyLWWMapCRDT.LWWEntry.class, classId++);
        registerable.register(PutOnlyLWWMapUpdate.class, classId++);
        registerable.register(PutOnlyLWWMapCRDT.class, classId++);
        registerable.register(PutOnlyLWWStringMapCRDT.class, classId++);
        registerable.register(PutOnlyLWWStringMapUpdate.class, classId++);

        registerable.register(AbstractAddOnlySetCRDT.class, classId++);
        registerable.register(AddOnlySetUpdate.class, classId++);
        registerable.register(AddOnlySetCRDT.class, classId++);
        registerable.register(AddOnlyStringSetCRDT.class, classId++);
        registerable.register(AddOnlyStringSetUpdate.class, classId++);

        registerable.register(AbstractAddWinsSetCRDT.class, classId++);
        registerable.register(AddWinsSetUpdate.class, classId++);
        registerable.register(AddWinsSetCRDT.class, classId++);
        registerable.register(AddWinsMessageSetCRDT.class, classId++);
        registerable.register(AddWinsMessageSetUpdate.class, classId++);

    }
}
