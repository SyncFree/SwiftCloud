package swift.client.proto;

import java.util.List;

import swift.crdt.operations.CRDTObjectOperationsGroup;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

// TODO this probably requires some rework 
public class UpdatesNotification implements RpcMessage {
    protected List<CRDTObjectOperationsGroup> objectUpdateGroups;

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((SwiftClient) handler).onReceive(conn, this);
    }
}
