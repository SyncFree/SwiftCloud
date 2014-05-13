package swift.pubsub;

import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTIdentifier;
import sys.pubsub.PubSub;
import sys.pubsub.PubSub.Notifyable;

public class SnapshotNotification implements Notifyable<CRDTIdentifier> {

    transient String clientId;
    transient CRDTIdentifier id;
    transient Timestamp timestamp;
    CausalityClock snapshotClock;
    CausalityClock estimatedDCVersion;
    CausalityClock estimatedDCStableVersion;

    SnapshotNotification() {
    }

    public SnapshotNotification(String clientId, CRDTIdentifier id, Timestamp timestamp, CausalityClock snapshotClock) {
        this.snapshotClock = snapshotClock;
        this.timestamp = timestamp;
        this.clientId = clientId;
        this.id = id;
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
}
