package swift.pubsub;

import java.util.HashSet;
import java.util.List;
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
 * A notification that brings client's cache from one consistent state to
 * another one.
 * 
 * @author mzawirski
 */
public class BatchUpdatesNotification implements Notifyable<CRDTIdentifier>, MetadataSamplable {
    // TODO: alternatively, for a best-effort FIFO session, include just last
    // id?
    protected CausalityClock oldVersion;
    protected CausalityClock newVersion;
    private boolean newVersionDisasterSafe;
    protected List<CRDTObjectUpdatesGroup<?>> updates;

    BatchUpdatesNotification() {
    }

    public BatchUpdatesNotification(CausalityClock oldVersion, CausalityClock newVersion, boolean disasterSafe,
            List<CRDTObjectUpdatesGroup<?>> updates) {
        this.oldVersion = oldVersion;
        this.newVersion = newVersion;
        this.newVersionDisasterSafe = disasterSafe;
        this.updates = updates;
    }

    @Override
    public void notifyTo(PubSub<CRDTIdentifier> pubsub) {
        ((SwiftSubscriber) pubsub).onNotification(this);
    }

    /**
     * @return old version assumed by the DC
     */
    public CausalityClock getOldVersion() {
        return oldVersion;
    }

    /**
     * @return new consistent version that these packet updates client's cache
     *         to
     */
    public CausalityClock getNewVersion() {
        return newVersion;
    }

    /**
     * @return all updates on client's subscribed object with timestamps between
     *         {@link #getOldVersion()} and {@link #getNewVersion()}
     */
    // FIXME: define subscribed objects with an epochId or so?
    public List<CRDTObjectUpdatesGroup<?>> getUpdates() {
        return updates;
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
        Set<CRDTIdentifier> keys = new HashSet<CRDTIdentifier>();
        for (final CRDTObjectUpdatesGroup<?> update : updates) {
            keys.add(update.getTargetUID());
        }
        return keys;
    }

    @Override
    public void recordMetadataSample(MetadataStatsCollector collector) {
        // TODO: Marek
    }
}
