package sys.net.api.rpc;

import sys.net.api.Endpoint;

/**
 * 
 * @author smd
 * 
 */
public interface RpcConnection {
    /**
     * Tells if this connection awaits a reply.
     * 
     * @return true/false if the connection awaits a reply or not
     */
    boolean expectingReply();

    /**
     * Send a (final) reply message using this connection
     * 
     * @param msg
     *            the reply being sent
     * @return true/false if the reply was successful or failed
     */
    boolean reply(final RpcMessage msg);

    /**
     * Send a reply message using this connection, with further message exchange
     * round implied.
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
