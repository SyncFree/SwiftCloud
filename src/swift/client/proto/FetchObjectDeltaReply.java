package swift.client.proto;

public class FetchObjectDeltaReply {
    public enum Status {
        OK, NOT_EXIST
    }

    // Strictly speaking, not everything from CRDTDelta may be necessary, so we
    // can try to use a specialized
    protected CRDTDelta delta;
    protected Status status;
}
