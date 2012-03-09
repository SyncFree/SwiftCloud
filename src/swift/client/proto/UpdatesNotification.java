package swift.client.proto;

import swift.crdt.CRDTIdentifier;

public class UpdatesNotification {
    protected CRDTIdentifier uid;
    protected CRDTDelta delta;
}
