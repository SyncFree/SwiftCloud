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
import sys.net.impl.KryoSerializer;

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
        final Kryo kryo = ((KryoSerializer) Networking.serializer()).kryo();

        kryo.register(ClientRequest.class);
        kryo.register(CommitUpdatesRequest.class);
        kryo.register(CommitUpdatesReply.class);
        kryo.register(CommitUpdatesReply.CommitStatus.class);
        kryo.register(FastRecentUpdatesRequest.class);
        kryo.register(FastRecentUpdatesReply.class);
        kryo.register(FastRecentUpdatesReply.ObjectSubscriptionInfo.class);
        kryo.register(FetchObjectDeltaRequest.class);
        kryo.register(FetchObjectVersionRequest.class);
        kryo.register(FetchObjectVersionReply.class);
        kryo.register(FetchObjectVersionReply.FetchStatus.class);
        kryo.register(GenerateTimestampRequest.class);
        kryo.register(GenerateTimestampReply.class);
        kryo.register(KeepaliveRequest.class);
        kryo.register(KeepaliveReply.class);
        kryo.register(LatestKnownClockRequest.class);
        kryo.register(LatestKnownClockReply.class);
        kryo.register(RecentUpdatesRequest.class);
        kryo.register(RecentUpdatesReply.class);
        kryo.register(SubscriptionType.class);
        kryo.register(UnsubscribeUpdatesRequest.class);

        kryo.register(Timestamp.class);
        kryo.register(TripleTimestamp.class);
        kryo.register(VersionVectorWithExceptions.class);
        kryo.register(VersionVectorWithExceptions.Pair.class);

        kryo.register(CRDTIdentifier.class);
        kryo.register(BaseCRDT.class);
        kryo.register(IntegerVersioned.class);
        kryo.register(IntegerVersioned.UpdatesPerSite.class);
        kryo.register(RegisterVersioned.class);
        kryo.register(RegisterVersioned.QueueEntry.class);
        kryo.register(SetIds.class);
        kryo.register(SetIntegers.class);
        kryo.register(SetMsg.class);
        kryo.register(SetStrings.class);
        kryo.register(SetVersioned.class);

        kryo.register(CRDTObjectOperationsGroup.class);
        kryo.register(BaseOperation.class);
        kryo.register(IntegerUpdate.class);
        kryo.register(RegisterUpdate.class);
        kryo.register(SetInsert.class);
        kryo.register(SetRemove.class);

        kryo.register(CommitTSRequest.class);
        kryo.register(CommitTSReply.class);
        kryo.register(DHTExecCRDT.class);
        kryo.register(DHTExecCRDTReply.class);
        kryo.register(DHTGetCRDT.class);
        kryo.register(DHTGetCRDTReply.class);
        kryo.register(DHTSendNotification.class);

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
