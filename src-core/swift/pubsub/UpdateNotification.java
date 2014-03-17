package swift.pubsub;

import swift.crdt.core.CRDTIdentifier;
import swift.proto.ObjectUpdatesInfo;
import sys.pubsub.PubSub;
import sys.pubsub.PubSub.Notifyable;
import sys.pubsub.PubSub.Subscriber;

public class UpdateNotification implements Notifyable<CRDTIdentifier> {

    public String clientId;
    public ObjectUpdatesInfo info;

    UpdateNotification() {
    }

    public UpdateNotification(String clientId, ObjectUpdatesInfo info) {
        this.info = info;
        this.clientId = clientId;
    }

    int n = 0;

    @Override
    public void notifyTo(PubSub<CRDTIdentifier> pubsub) {
        for (Subscriber<CRDTIdentifier> i : pubsub.subscribers(info.getId(), true))
            ((SwiftSubscriber) i).onNotification(this);
    }

    @Override
    public Object src() {
        return clientId;
    }
}
