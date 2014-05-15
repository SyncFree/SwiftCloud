package swift.pubsub;

import java.util.Set;

import swift.clocks.Timestamp;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.CRDTUpdate;
import swift.proto.MetadataSamplable;
import swift.proto.MetadataStatsCollector;
import swift.proto.ObjectUpdatesInfo;
import sys.pubsub.PubSub;
import sys.pubsub.PubSub.Notifyable;
import sys.pubsub.PubSub.Subscriber;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

public class UpdateNotification implements Notifyable<CRDTIdentifier>, MetadataSamplable {

    // TODO: lots of redundant things on the wire?
    public String srcId;
    public Timestamp timestamp;
    public ObjectUpdatesInfo info;

    UpdateNotification() {
    }

    public UpdateNotification(String srcId, Timestamp ts, ObjectUpdatesInfo info) {
        this.info = info;
        this.srcId = srcId;
        this.timestamp = ts;
    }

    public UpdateNotification(String srcId, Timestamp ts, Timestamp suTs, ObjectUpdatesInfo info) {
        this.info = info;
        this.srcId = srcId;
    }

    @Override
    public void notifyTo(PubSub<CRDTIdentifier> pubsub) {
        for (Subscriber<CRDTIdentifier> i : pubsub.subscribers(info.getId(), true))
            try {
                ((SwiftSubscriber) i).onNotification(this);
            } finally {
            }
    }

    @Override
    public Object src() {
        return srcId;
    }

    @Override
    public CRDTIdentifier key() {
        return info.getId();
    }

    @Override
    public Set<CRDTIdentifier> keys() {
        return null;
    }

    @Override
    public Timestamp timestamp() {
        return timestamp;
    }

    @Override
    public void recordMetadataSample(MetadataStatsCollector collector) {
        if (!collector.isEnabled()) {
            return;
        }
        Kryo kryo = collector.getFreshKryo();
        Output buffer = collector.getFreshKryoBuffer();

        // TODO: get it from the write, rather than recompute
        kryo.writeObject(buffer, this);
        final int totalSize = buffer.position();

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
        }
        final int updatesSize = buffer.position();

        kryo = collector.getFreshKryo();
        buffer = collector.getFreshKryoBuffer();
        for (final CRDTObjectUpdatesGroup<?> group : info.getUpdates()) {
            if (group.hasCreationState()) {
                kryo.writeObject(buffer, group.getCreationState());
            }
            if (group.getTargetUID() != null) {
                kryo.writeObject(buffer, group.getTargetUID());
            }
            for (final CRDTUpdate<?> op : group.getOperations()) {
                kryo.writeObject(buffer, op.getValueWithoutMetadata());
            }
        }
        final int valuesSize = buffer.position();
        collector.recordStats(this, totalSize, updatesSize, valuesSize);
    }
}
