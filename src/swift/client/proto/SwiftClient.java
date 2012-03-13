package swift.client.proto;


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
