package swift.client.proto;

import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

//TODO this probably requires some rework
public class UpdatesNotificationReply implements RpcMessage {
    protected boolean continueSubscription;

    @Override
    public void deliverTo(RpcConnection conn, RpcHandler handler) {
        ((UpdatesNotificationReplyHandler) handler).onReceive(conn, this);
    }
}
