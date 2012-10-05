package swift.utils;

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
    /**
     * Needs to be called exactly only once per JVM!
     */
    public static void init() {

        KryoLib.register(Timestamp.class, 0x50);
        KryoLib.register(TripleTimestamp.class, 0x51);
        KryoLib.register(VersionVectorWithExceptions.class, 0x52);
        KryoLib.register(VersionVectorWithExceptions.Interval.class, 0x53);

        KryoLib.register(CRDTIdentifier.class, 0x54);
        KryoLib.register(BaseCRDT.class, 0x55);
        KryoLib.register(IntegerVersioned.class, 0x56);
        KryoLib.register(RegisterVersioned.class, 0x58);
        KryoLib.register(RegisterVersioned.UpdateEntry.class, 0x59);
        KryoLib.register(SetIds.class, 0x60);
        KryoLib.register(SetIntegers.class, 0x61);
        KryoLib.register(SetMsg.class, 0x62);
        KryoLib.register(SetStrings.class, 0x63);
        KryoLib.register(SetVersioned.class, 0x64);

        KryoLib.register(ClientRequest.class, 0x70);
        KryoLib.register(CommitUpdatesRequest.class, 0x71);
        KryoLib.register(CommitUpdatesReply.class, 0x72);
        KryoLib.register(CommitUpdatesReply.CommitStatus.class, 0x73);
        KryoLib.register(FastRecentUpdatesRequest.class, 0x74);
        KryoLib.register(FastRecentUpdatesReply.class, 0x75);
        KryoLib.register(FastRecentUpdatesReply.ObjectSubscriptionInfo.class, 0x76);
        KryoLib.register(FetchObjectDeltaRequest.class, 0x77);
        KryoLib.register(FetchObjectVersionRequest.class, 0x78);
        KryoLib.register(FetchObjectVersionReply.class, 0x79);
        KryoLib.register(FetchObjectVersionReply.FetchStatus.class, 0x7A);
        KryoLib.register(GenerateTimestampRequest.class, 0x7B);
        KryoLib.register(GenerateTimestampReply.class, 0x7C);
        KryoLib.register(KeepaliveRequest.class, 0x7D);
        KryoLib.register(KeepaliveReply.class, 0x7E);
        KryoLib.register(LatestKnownClockRequest.class, 0x7F);
        KryoLib.register(LatestKnownClockReply.class, 0x80);
        KryoLib.register(RecentUpdatesRequest.class, 0x81);
        KryoLib.register(RecentUpdatesReply.class, 0x82);
        KryoLib.register(SubscriptionType.class, 0x83);
        KryoLib.register(UnsubscribeUpdatesRequest.class, 0x84);

        // KryoLib.register(CRDTObjectOperationsGroup.class, 0x85);
        // KryoLib.register(BaseOperation.class, 0x86);
        KryoLib.register(IntegerUpdate.class, 0x87);
        KryoLib.register(RegisterUpdate.class, 0x88);
        KryoLib.register(SetInsert.class, 0x89);
        KryoLib.register(SetRemove.class, 0x8A);

        KryoLib.register(CommitTSRequest.class, 0x8B);
        KryoLib.register(CommitTSReply.class, 0x8C);
        KryoLib.register(DHTExecCRDT.class, 0x8D);
        KryoLib.register(DHTExecCRDTReply.class, 0x8E);
        KryoLib.register(DHTGetCRDT.class, 0x8F);
        KryoLib.register(DHTGetCRDTReply.class, 0x90);
        KryoLib.register(DHTSendNotification.class, 0x91);

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
