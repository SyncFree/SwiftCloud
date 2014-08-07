package swift.pubsub;

import java.util.Set;

import swift.clocks.CausalityClock;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.CRDTUpdate;
import swift.proto.MetadataSamplable;
import swift.proto.MetadataStatsCollector;
import swift.proto.ObjectUpdatesInfo;
import swift.proto.SwiftProtocolHandler;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.pubsub.PubSub.Subscriber;
import sys.pubsub.PubSubNotification;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

public class UpdateNotification extends PubSubNotification<CRDTIdentifier> implements MetadataSamplable {

    // TODO: lots of redundant things on the wire?
    public ObjectUpdatesInfo info;
    public CausalityClock dcVersion;

    UpdateNotification() {
    }

    public UpdateNotification(Object srcId, ObjectUpdatesInfo info, CausalityClock dcVersion) {
        super(srcId);
        this.info = info;
        this.dcVersion = dcVersion;
    }

    @Override
    public CRDTIdentifier key() {
        return info.getId();
    }

    @Override
    public Set<CRDTIdentifier> keys() {
        return null;
    }

    public CausalityClock dcVersion() {
        return dcVersion;
    }

    @Override
    public void notifyTo(Subscriber<CRDTIdentifier> subscriber) {
        ((SwiftSubscriber) subscriber).onNotification(this);
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((SwiftProtocolHandler) handler).onReceive(this);
    }

    @Override
    public void recordMetadataSample(MetadataStatsCollector collector) {
        if (!collector.isMessageReportEnabled()) {
            return;
        }
        Kryo kryo = collector.getFreshKryo();
        Output buffer = collector.getFreshKryoBuffer();

        // TODO: get it from the write, rather than recompute
        kryo.writeObject(buffer, this);
        final int totalSize = buffer.position();

        int maxExceptionsNum = 0;
        int maxVectorSize = 0;

        kryo = collector.getFreshKryo();
        buffer = collector.getFreshKryoBuffer();
        if (info.getId() != null) {
            kryo.writeObject(buffer, info.getId());
        }
        for (final CRDTObjectUpdatesGroup<?> group : info.getUpdates()) {
            if (group.hasCreationState()) {
                kryo.writeObject(buffer, group.getCreationState());
            }
            if (group.getTargetUID() != null) {
                kryo.writeObject(buffer, group.getTargetUID());
            }
            for (final CRDTUpdate<?> op : group.getOperations()) {
                kryo.writeObject(buffer, op);
            }

            if (group.getDependency() != null) {
                maxExceptionsNum = Math.max(group.getDependency().getExceptionsNumber(), maxExceptionsNum);
                maxVectorSize = Math.max(group.getDependency().getSize(), maxVectorSize);
            }
        }
        final int updatesSize = buffer.position();

        int numberOfOps = 0;
        kryo = collector.getFreshKryo();
        buffer = collector.getFreshKryoBuffer();
        for (final CRDTObjectUpdatesGroup<?> group : info.getUpdates()) {
            if (group.hasCreationState()) {
                kryo.writeObject(buffer, group.getCreationState().getValue());
            }
            if (group.getTargetUID() != null) {
                kryo.writeObject(buffer, group.getTargetUID());
            }
            for (final CRDTUpdate<?> op : group.getOperations()) {
                kryo.writeObject(buffer, op.getValueWithoutMetadata());
                numberOfOps++;
            }
        }
        final int valuesSize = buffer.position();
        // FIXME
        int globalMetadata = 0;
        collector.recordMessageStats(this, totalSize, updatesSize, valuesSize, globalMetadata, numberOfOps, maxVectorSize,
                maxExceptionsNum);
    }

}
