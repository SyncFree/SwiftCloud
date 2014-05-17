package swift.pubsub;

import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTIdentifier;
import swift.proto.MetadataSamplable;
import swift.proto.MetadataStatsCollector;
import sys.pubsub.PubSub;
import sys.pubsub.PubSub.Notifyable;

public class SnapshotNotification implements Notifyable<CRDTIdentifier>, MetadataSamplable {

    transient String clientId;
    transient CRDTIdentifier id;
    transient Timestamp timestamp;
    CausalityClock snapshotClock;
    CausalityClock estimatedDCVersion;
    CausalityClock estimatedDCStableVersion;

    SnapshotNotification() {
    }

    public SnapshotNotification(CausalityClock snapshotClock, CausalityClock estimatedDCVersion,
            CausalityClock estimatedDCStableVersion) {
        this.snapshotClock = snapshotClock;
        this.estimatedDCVersion = estimatedDCVersion;
        this.estimatedDCStableVersion = estimatedDCStableVersion;
    }

    @Override
    public void notifyTo(PubSub<CRDTIdentifier> pubsub) {
        ((SwiftSubscriber) pubsub).onNotification(this);
    }

    @Override
    public Object src() {
        return clientId;
    }

    public CausalityClock estimatedDCVersion() {
        return estimatedDCVersion;
    }

    public CausalityClock estimatedDCStableVersion() {
        return estimatedDCStableVersion;
    }

    public CausalityClock snapshotClock() {
        return snapshotClock;
    }

    public Timestamp timestamp() {
        return timestamp;
    }

    @Override
    public CRDTIdentifier key() {
        return null;
    }

    public String toString() {
        return snapshotClock.toString();
    }

    @Override
    public Set<CRDTIdentifier> keys() {
        return null;
    }

    @Override
    public void recordMetadataSample(MetadataStatsCollector collector) {
        if (!collector.isEnabled()) {
            return;
        }
        final Kryo kryo = collector.getFreshKryo();
        final Output buffer = collector.getFreshKryoBuffer();

        int maxExceptionsNum = 0;
        if (snapshotClock != null) {
            maxExceptionsNum = Math.max(snapshotClock.getExceptionsNumber(), maxExceptionsNum);
        }
        if (estimatedDCVersion != null) {
            maxExceptionsNum = Math.max(estimatedDCVersion.getExceptionsNumber(), maxExceptionsNum);
        }
        if (estimatedDCStableVersion != null) {
            maxExceptionsNum = Math.max(estimatedDCStableVersion.getExceptionsNumber(), maxExceptionsNum);
        }

        // TODO: capture from the wire, rather than recompute here
        kryo.writeObject(buffer, this);
        collector.recordStats(this, buffer.position(), 0, 0, maxExceptionsNum);
    }
}
