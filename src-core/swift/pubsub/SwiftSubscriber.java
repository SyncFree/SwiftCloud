package swift.pubsub;

import swift.crdt.core.CRDTIdentifier;
import sys.pubsub.PubSub.Subscriber;
import sys.pubsub.PubSubNotification;

public interface SwiftSubscriber extends Subscriber<CRDTIdentifier> {
    void onNotification(UpdateNotification evt);

    void onNotification(BatchUpdatesNotification evt);

    void onNotification(PubSubNotification<CRDTIdentifier> evt);
}
