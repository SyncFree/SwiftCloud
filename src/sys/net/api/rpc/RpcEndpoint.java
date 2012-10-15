package sys.net.api.rpc;

import sys.net.api.Endpoint;

/**
 * 
 * Represents a local endpoint listening for incoming rpc messages.
 * 
 * @author smd
 * 
 */
public interface RpcEndpoint {

	/**
	 * The local endpoint information (ip + tcp port)
	 * 
	 * @return
	 */
	Endpoint localEndpoint();

	/**
	 * 
	 * Sends an invocation message to a (listening) destination endpoint
	 * 
	 * @param dst
	 *            the destination of the invocation message, blocking until the
	 *            message is written to the underlying channel.
	 * @param m
	 *            the message being sent
	 * @return the handle associated for the message
	 */
	RpcHandle send(final Endpoint dst, final RpcMessage m);

	/**
	 * Sends an invocation message to a (listening) destination endpoint,
	 * blocking until a reply is received (or the default timeout expires).
	 * 
	 * @param dst
	 *            the destination of the invocation message
	 * @param m
	 *            the message being sent
	 * @param replyHandler
	 *            - the handler for processing the reply message
	 * @return the handle associated for the message
	 */
	RpcHandle send(final Endpoint dst, final RpcMessage m, final RpcHandler replyHandler);

	/**
	 * Sends an invocation message to a (listening) destination endpoint,
	 * blocking until a reply is received or the timeout expires.
	 * 
	 * @param dst
	 *            the destination of the invocation message
	 * @param m
	 *            the message being sent
	 * @param replyHandler
	 *            - the handler for processing the reply message
	 * 
	 * @param timout
	 *            - number of milliseconds to wait for the reply. <= 0 returns
	 *            immediately. FIXME: document MAX_TIMEOUT
	 * @return the handle associated for the message
	 */
	RpcHandle send(final Endpoint dst, final RpcMessage m, final RpcHandler replyHandler, int timeout);

	/**
	 * Sets the handler responsible for processing incoming invocation messages
	 * 
	 * @param handler
	 *            the handler for processing invocation messages
	 * @return itself
	 */
	<T extends RpcEndpoint> T setHandler(final RpcHandler handler);

	/**
	 * Obtains a reference to the RPC factory used to obtain this endpoint.
	 * 
	 * @return the reference to the factory that created this endpoint.
	 */
	RpcFactory getFactory();

	/**
	 * Sets the default timeout for this endpoint, while waiting for a reply to a message sent.
	 * @param ms - the new timeout value to be used in milliseconds.
	 */
	void setDefaultTimeout( int ms ) ;

	/**
	 * Obtains the default timeout in use for this endpoint, while waiting for a reply to a message sent.
	 * @return the timeout in milliseconds.
	 */
	int getDefaultTimeout();
}
