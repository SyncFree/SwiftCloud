package swift.client.proto;

import swift.crdt.CRDTIdentifier;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Client request to unsubscribe update notifications for an object.
 * 
 * @author mzawirski
 */
public class UnsubscribeUpdatesRequest implements RpcMessage {
    protected CRDTIdentifier uid;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public UnsubscribeUpdatesRequest() {
    }

    public UnsubscribeUpdatesRequest(CRDTIdentifier uid) {
        this.uid = uid;
    }

    /**
     * @return object id to unsubscribe
     */
    public CRDTIdentifier getUid() {
        return uid;
    }

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }
}
