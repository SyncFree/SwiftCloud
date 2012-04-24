package sys.dht.api;

import sys.net.api.Endpoint;

/**
 * The public interface of the DHT.
 * 
 * @author smd (smd@fct.unl.pt)
 * 
 */
public interface DHT {

    Endpoint localEndpoint();
    
	void send(final DHT.Key key, final DHT.Message msg);

	void send(final DHT.Key key, final DHT.Message msg, DHT.ReplyHandler handler);

	interface Key {

		long longHashValue();
	}

	interface Message {

		void deliverTo(final DHT.Connection conn, final DHT.Key key, final DHT.MessageHandler handler);

	}

	interface MessageHandler {

		void onFailure();

		void onReceive(final DHT.Connection conn, final DHT.Key key, final DHT.Message request);

	}

	interface Reply {

		void deliverTo(final DHT.Connection conn, final DHT.ReplyHandler handler);

	}

	interface ReplyHandler {

		void onFailure();

		void onReceive(final DHT.Reply msg);

		void onReceive(final DHT.Connection conn, final DHT.Reply reply);
	}

	interface Connection {

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
		boolean reply(final DHT.Reply msg);

		/**
		 * Send a reply message using this connection, with further message
		 * exchange round implied.
		 * 
		 * @param msg
		 *            the reply message
		 * @param handler
		 *            the handler that will be notified upon the arrival of an
		 *            reply (to this reply)
		 * @return true/false if the reply was successful or failed
		 */
		boolean reply(final DHT.Reply msg, final DHT.ReplyHandler handler);

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

	abstract class AbstractReplyHandler implements ReplyHandler {

		@Override
		public void onFailure() {
		}

		@Override
		public void onReceive(DHT.Reply msg) {
		}

		@Override
		public void onReceive(DHT.Connection conn, DHT.Reply reply) {
		}
	}

	abstract class AbstractMessageHandler implements MessageHandler {

		@Override
		public void onFailure() {
		}

		@Override
		public void onReceive(DHT.Connection conn, DHT.Key key, DHT.Message request) {
		}
	}
}
