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
	 * 			the endpoint that is bound to the service
	 */
	RpcEndpoint toService( int service, RpcHandler handler);

	/**
	 * Creates a connection to service for accepting and sending messages according to a
	 * simple RPC scheme. Allows for a sequence of cascading send/reply message
	 * exchanges.
	 * 
	 * @param service
	 *            the id of the service that will send/receive messages
	 * @param handler
	 *            the handler used for message delivery for this service
	 * @return
	 * 			the endpoint that is bound to the service
	 */
	RpcEndpoint toService( int service);

	/**
	 * Creates a connection to service for accepting and sending messages according to a
	 * simple RPC scheme. Allows for a sequence of cascading send/reply message
	 * exchanges.
	 * 
	 * @param service
	 *            the id of the service that will send/receive messages
	 * @param handler
	 *            the handler used for message delivery for this service
	 * @return
	 * 			the endpoint that is bound to the default service (zero)
	 * 
	 */
	RpcEndpoint toDefaultService();

}
