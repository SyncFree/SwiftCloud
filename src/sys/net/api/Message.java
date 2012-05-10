package sys.net.api;

/**
 * This interface is used for delivering incoming messages.
 * 
 * The underlying communication system upon receiving a message will invoke the
 * deliverTo method in the message object. A Visitor programming pattern can be
 * used to call a specific handler for the message class.
 * 
 * @author smd (smd@fct.unl.pt)
 * 
 */
public interface Message {

	/**
	 * Implement this method by casting the handler to a more specific class,
	 * with an onReceive(...) method for this particular object.
	 * 
	 * @param conn
	 *            The connection that received the message, which may be used to
	 *            sending a reply back.
	 * @param handler
	 *            The handler the message should be delivered to.
	 */
	void deliverTo(final TransportConnection conn, final MessageHandler handler);

}
