package swift.client.proto;

import swift.crdt.CRDTIdentifier;

/**
 * Client request to unsubscribe notifications for an object.
 * 
 * @author mzawirski
 */
public class UnsubscribeNotificationsRequest {
    protected CRDTIdentifier uid;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public UnsubscribeNotificationsRequest() {
    }

    public UnsubscribeNotificationsRequest(CRDTIdentifier uid) {
        this.uid = uid;
    }

    /**
     * @return object id to unsubscribe
     */
    public CRDTIdentifier getUid() {
        return uid;
    }
}
