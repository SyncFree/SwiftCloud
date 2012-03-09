package swift.client.proto;

import swift.crdt.CRDTIdentifier;

/**
 * Rewrite in SwiftServer manner.
 * 
 * @author mzawirski
 */
public interface SwiftClient {
    /**
     */
    UpdatesNotificationReply notifyNewUpdates(UpdatesNotification notification);
}
