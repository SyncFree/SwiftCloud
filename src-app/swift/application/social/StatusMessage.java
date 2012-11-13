package swift.application.social;

import swift.crdt.CRDTIdentifier;

public class StatusMessage {
    CRDTIdentifier from;
    long timestamp;
    String text;
}
