package sys.net.api.rpc;

/**
 * 
 * Interface for specifying the messages exchanged in a basic rpc service
 * 
 * @author smd
 * 
 */
public interface RpcMessage {

	void deliverTo(final RpcConnection conn, final RpcHandler handler);

}
