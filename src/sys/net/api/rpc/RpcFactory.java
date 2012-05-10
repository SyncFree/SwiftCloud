package sys.net.api.rpc;

public interface RpcFactory {

	/**
	 * Creates a service for accepting and sending messages according to a
	 * simple RPC scheme. Allows for a sequence of cascading send/reply message
	 * exchanges.
	 * 
	 * @param service
	 *            the id of the service that will send/receive messages
	 * @param handler
	 *            the handler used for message delivery for this service
	 * @return
	 */
	RpcEndpoint rpcService(final int service, final RpcHandler handler);

	/**
	 * Creates a service for accepting and sending messages according to a
	 * simple RPC scheme. Allows for a sequence of cascading send/reply message
	 * exchanges.
	 * 
	 * @param service
	 *            the id of the service that will send/receive messages
	 * @param handler
	 *            the handler used for message delivery for this service
	 * @return
	 */
	RpcEndpoint rpcServiceConnect(final int service);

}
