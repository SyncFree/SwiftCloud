package swift.client.proto;

/**
 * Type of updates subscription.
 * 
 * @author nmp, mzawirski
 */
public enum SubscriptionType {
    /**
     * Receive updates on changes.
     */
    UPDATES,
    /**
     * Receive a single notification on changes.
     */
    NOTIFICATION,
    /**
     * Receive nothing on changes.
     */
    NONE
}
