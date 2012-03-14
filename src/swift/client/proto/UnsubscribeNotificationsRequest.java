package swift.client.proto;

import swift.crdt.CRDTIdentifier;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

/**
 * Client request to unsubscribe notifications for an object.
 * 
 * @author mzawirski
 */
public class UnsubscribeNotificationsRequest implements RpcMessage {
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

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((SwiftServer) handler).onReceive(conn, this);
    }
}
