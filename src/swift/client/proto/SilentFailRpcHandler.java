package swift.client.proto;

import java.util.logging.Logger;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class SilentFailRpcHandler implements RpcHandler {
    private static Logger logger = Logger.getLogger(SilentFailRpcHandler.class.getName());

    @Override
    public void onReceive(RpcMessage m) {
        logger.warning("unhandled RPC message " + m);
    }

    @Override
    public void onReceive(RpcHandle handle, RpcMessage m) {
        logger.warning("unhandled RPC message " + m);
    }

    @Override
    public void onFailure( RpcHandle h) {
    }

}
