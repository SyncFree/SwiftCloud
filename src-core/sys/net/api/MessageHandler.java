package sys.net.api;

/**
 * 
 * The generic handler called to received incoming messages. This interface is
 * meant to be extended with specific onReceive methods for the expected
 * incoming messages. It also requires that messages implement the deliverTo
 * method, accordingly.
 * 
 * @author smd
 * 
 */
public interface MessageHandler {

	void onAccept(final TransportConnection conn);

	void onConnect(final TransportConnection conn);

	void onFailure(final TransportConnection conn);

	void onClose(final TransportConnection conn);

	/**
	 * Called whenever a connection to a remote endpoint cannot be established
	 * or fails.
	 * 
	 * @param dst
	 *            - the remote endpoint
	 * @param m
	 *            - the message that was being sent
	 */
	void onFailure(final Endpoint dst, final Message m);

	/**
	 * Called to upon the arrival of a message in the given connection.
	 * 
	 * @param conn
	 *            the connection that received the message, which may be used to
	 *            send a reply back.
	 * @param m
	 *            the message received in the connection
	 */
	void onReceive(final TransportConnection conn, final Message m);
}
