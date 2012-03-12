package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;

public class FetchObjectVersionRequest {
    protected CRDTIdentifier uid;
    protected CausalityClock version;
    protected boolean create;
    protected boolean subscribeUpdates;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public FetchObjectVersionRequest() {
    }
    
    public FetchObjectVersionRequest(CRDTIdentifier uid, CausalityClock version, boolean create,
            boolean subscribeUpdates) {
        this.uid = uid;
        this.version = version;
        this.create = create;
        this.subscribeUpdates = subscribeUpdates;
    }

    public CRDTIdentifier getUid() {
        return uid;
    }

    public CausalityClock getVersion() {
        return version;
    }

    public boolean isCreate() {
        return create;
    }

    public boolean isSubscribeUpdates() {
        return subscribeUpdates;
    }
}
