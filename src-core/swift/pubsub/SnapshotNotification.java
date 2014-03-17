package swift.pubsub;

import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTIdentifier;
import sys.pubsub.PubSub;
import sys.pubsub.PubSub.Notifyable;
import sys.pubsub.PubSub.Subscriber;

public class SnapshotNotification implements Notifyable<CRDTIdentifier> {

    String clientId;
    Timestamp timestamp;
    Set<CRDTIdentifier> uids;
    CausalityClock snapshotClock;

    SnapshotNotification() {
    }

    public SnapshotNotification(String clientId, Set<CRDTIdentifier> uids, Timestamp timestamp,
            CausalityClock snapshotClock) {
        this.snapshotClock = snapshotClock;
        this.timestamp = timestamp;
        this.clientId = clientId;
        this.uids = uids;
    }

    @Override
    public void notifyTo(PubSub<CRDTIdentifier> pubsub) {
        for (Subscriber<CRDTIdentifier> i : pubsub.subscribers(uids))
            ((SwiftSubscriber) i).onNotification(this);
    }

    @Override
    public Object src() {
        return clientId;
    }

    public CausalityClock snapshotClock() {
        return snapshotClock;
    }

    public Timestamp timestamp() {
        return timestamp;
    }
}
