package swift.pubsub;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.CRDTUpdate;
import swift.proto.MetadataSamplable;
import swift.proto.MetadataStatsCollector;
import sys.pubsub.PubSub;
import sys.pubsub.PubSub.Notifyable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

/**
 * A notification that brings client's cache (a set of subscribed objects) from
 * the previous consistent state to the new one, described by a vector.
 * 
 * @author mzawirski,smd
 */
public class BatchUpdatesNotification implements Notifyable<CRDTIdentifier>, MetadataSamplable {
    protected CausalityClock newVersion;
    private boolean newVersionDisasterSafe;
    protected Map<CRDTIdentifier, List<CRDTObjectUpdatesGroup<?>>> objectsUpdates;

    BatchUpdatesNotification() {
    }

    public BatchUpdatesNotification(CausalityClock newVersion, boolean disasterSafe,
            Map<CRDTIdentifier, List<CRDTObjectUpdatesGroup<?>>> objectsUpdates) {
        this.newVersion = newVersion;
        this.newVersionDisasterSafe = disasterSafe;
        this.objectsUpdates = objectsUpdates;
    }

    @Override
    public void notifyTo(PubSub<CRDTIdentifier> pubsub) {
        ((SwiftSubscriber) pubsub).onNotification(this);
    }

    /**
     * @return new consistent version that these packet updates client's cache
     *         to
     */
    public CausalityClock getNewVersion() {
        return newVersion;
    }

    /**
     * @return a map of all updates on the objects subscribed by the client,
     *         with timestamps between {@link #getNewVersion()} of the previous
     *         notification, and {@link #getNewVersion()} of this one
     */
    // FIXME: define subscribed objects with an epochId or so?
    public Map<CRDTIdentifier, List<CRDTObjectUpdatesGroup<?>>> getObjectsUpdates() {
        return objectsUpdates;
    }

    /**
     * @return true if the version is disaster safe
     */
    public boolean isNewVersionDisasterSafe() {
        return newVersionDisasterSafe;
    }

    @Override
    public Timestamp timestamp() {
        return null;
    }

    @Override
    public Object src() {
        return null;
    }

    @Override
    public CRDTIdentifier key() {
        return null;
    }

    @Override
    public Set<CRDTIdentifier> keys() {
        return Collections.unmodifiableSet(objectsUpdates.keySet());
    }

    @Override
    public void recordMetadataSample(MetadataStatsCollector collector) {
        if (!collector.isEnabled()) {
            return;
        }
        Kryo kryo = collector.getFreshKryo();
        Output buffer = collector.getFreshKryoBuffer();

        // TODO: get it from the wire, rather than recompute
        kryo.writeObject(buffer, this);
        final int totalSize = buffer.position();

        kryo = collector.getFreshKryo();
        buffer = collector.getFreshKryoBuffer();
        for (final Entry<CRDTIdentifier, List<CRDTObjectUpdatesGroup<?>>> entry : objectsUpdates.entrySet()) {
            kryo.writeObject(buffer, entry.getKey());
            for (final CRDTObjectUpdatesGroup<?> group : entry.getValue()) {
                if (group.hasCreationState()) {
                    kryo.writeObject(buffer, group.getCreationState());
                }
                kryo.writeObject(buffer, group.getOperations());
            }
        }
        final int updatesSize = buffer.position();

        kryo = collector.getFreshKryo();
        buffer = collector.getFreshKryoBuffer();
        for (final Entry<CRDTIdentifier, List<CRDTObjectUpdatesGroup<?>>> entry : objectsUpdates.entrySet()) {
            kryo.writeObject(buffer, entry.getKey());
            for (final CRDTObjectUpdatesGroup<?> group : entry.getValue()) {
                if (group.hasCreationState()) {
                    final Object value = group.getCreationState().getValue();
                    if (value != null) {
                        kryo.writeObject(buffer, value);
                    } else {
                        kryo.writeObject(buffer, false);
                    }
                }
                for (final CRDTUpdate<?> op : group.getOperations()) {
                    final Object value = op.getValueWithoutMetadata();
                    if (value != null) {
                        kryo.writeObject(buffer, value);
                    } else {
                        kryo.writeObject(buffer, false);
                    }
                }
            }
        }
        final int valuesSize = buffer.position();

        final int maxExceptionsNum = newVersion.getExceptionsNumber();
        collector.recordStats(this, totalSize, updatesSize, valuesSize, maxExceptionsNum);
    }

    @Override
    public String toString() {
        return "BatchUpdatesNotification [newVersion=" + newVersion + ", newVersionDisasterSafe="
                + newVersionDisasterSafe + ", objectsUpdates=" + objectsUpdates + "]";
    }
}
