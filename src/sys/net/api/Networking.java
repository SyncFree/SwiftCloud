package sys.net.api;

import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcFactory;
import sys.net.api.rpc.RpcHandler;

/**
 * Used to obtain endpoints form performing message exchange.
 * 
 * @author smd
 * 
 */
abstract public class Networking {

	/**
	 * Creates a local endpoint for accepting and sending messages
	 * 
	 * @param tcpPort
	 *            the port used for listening tcp connections
	 * @return the endpoint created
	 */
	abstract public Endpoint bind(final int tcpPort);

	/**
	 * Creates an endpoint pointing to a remote location, identified by host/ip
	 * and a port. Connections are creating upon calling one of the send
	 * methods.
	 * 
	 * @param host
	 *            - the host/ip address of the remote location
	 * @param tcpPort
	 *            - the port for establishing connections
	 * @return the endpoint created
	 */
	abstract public Endpoint resolve(final String host, final int tcpPort);

	/**
	 * Creates a local endpoint form accepting and sending messages according to
	 * a simple RPC scheme. Allows for a sequence of cascading send/reply
	 * message exchanges.
	 * 
	 * @param tcpPort
	 *            the listening port
	 * @param handler
	 *            the handler used for message delivery
	 * @return
	 */
	abstract public RpcEndpoint rpcBind(final int tcpPort, final RpcHandler handler);

	/**
	 * Creates a rpc factory, which allows to register handler associted with
	 * numbered rpc services.
	 * 
	 * @param tcpPort
	 *            the tcpPort used to send/receive messages for the rpc factory
	 * @return
	 */
	abstract public RpcFactory rpcBind(final int tcpPort);

	/**
	 * Obtains a singleton instance of a serializer object
	 * @return the serializer
	 */
	abstract public Serializer serializer();

	protected Networking() {
		Networking = this;
	}

	/**
	 * Upon proper initialization should point the an instance implementation.
	 * 
	 * Intended for use with static import that allow instance methods that
	 * mimic the use of static class methods. It should be possible to
	 * initialize with a different implementation, such as one meant for
	 * simulated environment.
	 */
	public static Networking Networking;

}
