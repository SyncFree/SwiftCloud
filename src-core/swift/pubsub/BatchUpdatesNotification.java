package swift.pubsub;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.proto.MetadataSamplable;
import swift.proto.MetadataStatsCollector;
import sys.pubsub.PubSub;
import sys.pubsub.PubSub.Notifyable;

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
            List<CRDTObjectUpdatesGroup<?>> updates) {
        this.newVersion = newVersion;
        this.newVersionDisasterSafe = disasterSafe;
        this.objectsUpdates = new HashMap<CRDTIdentifier, List<CRDTObjectUpdatesGroup<?>>>();
        for (final CRDTObjectUpdatesGroup update : updates) {
            final CRDTIdentifier id = update.getTargetUID();
            List<CRDTObjectUpdatesGroup<?>> objectUpdates = objectsUpdates.get(id);
            if (objectUpdates == null) {
                objectUpdates = new LinkedList<CRDTObjectUpdatesGroup<?>>();
                objectsUpdates.put(id, objectUpdates);
            }
            objectUpdates.add(update.strippedWithCopiedTimestampMappings());
        }
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
        // TODO: Marek
    }
}
