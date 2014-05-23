package swift.pubsub;

import swift.crdt.core.CRDTIdentifier;
import sys.pubsub.PubSub.Subscriber;

public interface SwiftSubscriber extends Subscriber<CRDTIdentifier> {

    public void onNotification(SwiftNotification event);

    public void onNotification(UpdateNotification update);

    public void onNotification(BatchUpdatesNotification snapshot);
}
