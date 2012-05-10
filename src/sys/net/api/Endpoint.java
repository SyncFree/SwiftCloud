package sys.net.api;

/**
 * Endpoints are intended for message exchange, using an underlying serializer
 * (currently Kryo)
 * 
 * Use the Networking class to create endpoints. Endpoints can also received in
 * messages.
 * 
 * Incoming messages are delivered by an upcall to the registered handler.
 * Failures are also notified asynchronously via the upcall handler.
 * 
 * @author smd
 * 
 */
public interface Endpoint {

	/**
	 * Establishes a tcp connection to a remote endpoint.
	 * 
	 * @param dst
	 *            Remote endpoint of the connection
	 * @return an object representing the connection to the endpoint, allowing
	 *         for message exchange.
	 */
	TransportConnection connect(final Endpoint dst);

	/**
	 * Sends a message to a remote endpoint, after successfully establishing a
	 * tcp connection.
	 * 
	 * @param dst
	 *            Remote endpoint that will receive the message
	 * @param m
	 *            The message being sent
	 * @return an object representing the connection to the endpoint, allowing
	 *         for further message exchange.
	 */
	TransportConnection send(final Endpoint dst, final Message m);

	/**
	 * Sets the handler for incoming messages
	 * 
	 * @param handler
	 */
	void setHandler(final MessageHandler handler);

	/**
	 * Gets the handler for incoming messages
	 * 
	 * @param handler
	 */
	MessageHandler getHandler();

	/**
	 * Obtains the endpoint locator, ie., an opaque and compact representation
	 * of the endpoint. Used to serialized endpoints.
	 * 
	 * @return the locator for this endpoint
	 */
	<T> T locator();

}
