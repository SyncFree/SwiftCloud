package sys.net.api.rpc;

import sys.net.api.Endpoint;

/**
 * 
 * @author smd
 * 
 */
public interface RpcConnection {
	/**
	 * Tells if the sender awaits a reply.
	 * 
	 * @return true/false if the sender awaits a reply or not
	 */
	boolean expectingReply();

	/**
	 * Send a (final) reply message using this connection.
	 * 
	 * @param msg
	 *            the reply being sent
	 * @return true/false if the reply was successful or failed
	 */
	boolean reply(final RpcMessage msg);

	/**
	 * Send a reply message using this connection, with a further message
	 * exchange round implied. Blocks the calling thread until the reply is
	 * received, and the calling thread will invoke the handler.
	 * 
	 * @param msg
	 *            the reply message
	 * @param handler
	 *            the handler that will be notified upon the arrival of an reply
	 *            (to this reply)
	 * @return true/false if the reply was successful or failed
	 */
	boolean reply(final RpcMessage msg, final RpcHandler handler);

	/**
	 * Send a reply message using this connection, with a further message
	 * exchange round implied. If the timeout is greater than 0, blocks the
	 * calling thread until the reply is received, and the calling thread will
	 * invoke the handler. If the timeout is 0, returns without waiting for the
	 * reply, and the reply handler will be invoked in another thread.
	 * 
	 * @param msg
	 *            the reply message
	 * @param handler
	 *            the handler that will be notified upon the arrival of an reply
	 *            (to this reply)
	 * 
	 * @param timeout
	 *            number of milliseconds to block waiting for a reply. 0 - means
	 *            no blocking/asynchronous. The handled will be called by a
	 *            different thread.
	 * @return true/false if the reply was successful or failed
	 */
	boolean reply(final RpcMessage msg, final RpcHandler handler, int timeout);

	/**
	 * 
	 * @return true if the connection failed to establish or failed during
	 *         message exchange
	 */
	boolean failed();

	/**
	 * Optional method to dispose of a connection
	 */
	void dispose();

	/**
	 * Obtains the remote endpoint of this connection
	 * 
	 * @return the remote endpoint of this connection
	 */
	Endpoint remoteEndpoint();

}
