package swift.utils;

import static sys.net.api.Networking.Networking;
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
import swift.crdt.operations.BaseOperation;
import swift.crdt.operations.CRDTObjectOperationsGroup;
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

import com.esotericsoftware.kryo.Kryo;

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

        KryoLib.register(ClientRequest.class);
        KryoLib.register(CommitUpdatesRequest.class);
        KryoLib.register(CommitUpdatesReply.class);
        KryoLib.register(CommitUpdatesReply.CommitStatus.class);
        KryoLib.register(FastRecentUpdatesRequest.class);
        KryoLib.register(FastRecentUpdatesReply.class);
        KryoLib.register(FastRecentUpdatesReply.ObjectSubscriptionInfo.class);
        KryoLib.register(FetchObjectDeltaRequest.class);
        KryoLib.register(FetchObjectVersionRequest.class);
        KryoLib.register(FetchObjectVersionReply.class);
        KryoLib.register(FetchObjectVersionReply.FetchStatus.class);
        KryoLib.register(GenerateTimestampRequest.class);
        KryoLib.register(GenerateTimestampReply.class);
        KryoLib.register(KeepaliveRequest.class);
        KryoLib.register(KeepaliveReply.class);
        KryoLib.register(LatestKnownClockRequest.class);
        KryoLib.register(LatestKnownClockReply.class);
        KryoLib.register(RecentUpdatesRequest.class);
        KryoLib.register(RecentUpdatesReply.class);
        KryoLib.register(SubscriptionType.class);
        KryoLib.register(UnsubscribeUpdatesRequest.class);

        KryoLib.register(Timestamp.class);
        KryoLib.register(TripleTimestamp.class);
        KryoLib.register(VersionVectorWithExceptions.class);
        KryoLib.register(VersionVectorWithExceptions.Pair.class);

        KryoLib.register(CRDTIdentifier.class);
        KryoLib.register(BaseCRDT.class);
        KryoLib.register(IntegerVersioned.class);
        KryoLib.register(IntegerVersioned.UpdatesPerSite.class);
        KryoLib.register(RegisterVersioned.class);
        KryoLib.register(RegisterVersioned.QueueEntry.class);
        KryoLib.register(SetIds.class);
        KryoLib.register(SetIntegers.class);
        KryoLib.register(SetMsg.class);
        KryoLib.register(SetStrings.class);
        KryoLib.register(SetVersioned.class);

        KryoLib.register(CRDTObjectOperationsGroup.class);
        KryoLib.register(BaseOperation.class);
        KryoLib.register(IntegerUpdate.class);
        KryoLib.register(RegisterUpdate.class);
        KryoLib.register(SetInsert.class);
        KryoLib.register(SetRemove.class);

        KryoLib.register(CommitTSRequest.class);
        KryoLib.register(CommitTSReply.class);
        KryoLib.register(DHTExecCRDT.class);
        KryoLib.register(DHTExecCRDTReply.class);
        KryoLib.register(DHTGetCRDT.class);
        KryoLib.register(DHTGetCRDTReply.class);
        KryoLib.register(DHTSendNotification.class);

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
