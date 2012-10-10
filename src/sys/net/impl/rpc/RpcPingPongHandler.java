package sys.net.impl.rpc;

import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;

public interface RpcPingPongHandler extends MessageHandler {

    public void onReceive(final TransportConnection conn, final RpcPing ping);

    public void onReceive(final TransportConnection conn, final RpcPong pong) ;

}
