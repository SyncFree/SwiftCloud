package swift.pubsub;

import java.util.Set;

import swift.clocks.Timestamp;
import swift.crdt.core.CRDTIdentifier;
import swift.proto.ObjectUpdatesInfo;
import sys.pubsub.PubSub;
import sys.pubsub.PubSub.Notifyable;
import sys.pubsub.PubSub.Subscriber;

public class UpdateNotification implements Notifyable<CRDTIdentifier> {

    public String srcId;
    public Timestamp timestamp;
    public ObjectUpdatesInfo info;
    transient public SwiftNotification wrapper;

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
}
