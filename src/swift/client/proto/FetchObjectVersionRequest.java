package swift.client.proto;

import swift.clocks.CausalityClock;
import swift.crdt.CRDTIdentifier;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Client request to fetch a particular version of an object.
 * 
 * @author mzawirski
 */
public class FetchObjectVersionRequest implements RpcMessage {
    protected CRDTIdentifier uid;
    protected CausalityClock version;
    protected boolean subscribeUpdatesRequest;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public FetchObjectVersionRequest() {
    }

    public FetchObjectVersionRequest(CRDTIdentifier uid, CausalityClock version, boolean subscribeUpdates) {
        this.uid = uid;
        this.version = version;
        this.subscribeUpdatesRequest = subscribeUpdates;
    }

    /**
     * @return id of the requested object
     */
    public CRDTIdentifier getUid() {
        return uid;
    }

    /**
     * @return minimum version requested; null if client requests the most
     *         recent version
     */
    public CausalityClock getVersion() {
        return version;
    }

    /**
     * @return true if client requests to subscribe updates for this object
     */
    public boolean isSubscribeUpdatesRequest() {
        return subscribeUpdatesRequest;
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }
}
