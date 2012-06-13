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
	 * Allows sending an invocation message to a (listening) destination
	 * endpoint
	 * 
	 * @param dst
	 *            the destination of the invocation message
	 * @param m
	 *            the message being sent
	 * @return the handle associated for the message
	 */
	RpcHandle send(final Endpoint dst, final RpcMessage m);

	/**
	 * Allows sending an invocation message to a (listening) destination
	 * endpoint, blocking until a reply is received.
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
	 * Allows sending an invocation message to a (listening) destination
	 * endpoint, blocking until a reply is received or the timeout expires.
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
	 * Allows setting the handler responsible for processing incoming invocation
	 * messages
	 * 
	 * @param handler
	 *            the handler for processing invocation messages
	 * @return itself
	 */
	<T extends RpcEndpoint> T setHandler(final RpcHandler handler);
}
