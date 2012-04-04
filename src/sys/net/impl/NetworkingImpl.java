package sys.net.impl;

import sys.net.api.Endpoint;
import sys.net.api.Networking;
import sys.net.api.Serializer;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcFactory;
import sys.net.api.rpc.RpcHandler;
import sys.net.impl.rpc.RpcFactoryImpl;

public class NetworkingImpl extends Networking {

	NetworkingImpl() {
	}

	public static void init() {
		new NetworkingImpl();
		KryoSerialization.init();
	}

	@Override
	synchronized public Endpoint bind(final int tcpPort) {
		return LocalEndpoint.open(tcpPort);
	}

	@Override
	public Endpoint resolve(final String host, final int tcpPort) {
		return new RemoteEndpoint(host, tcpPort);
	}

	@Override
	synchronized public RpcEndpoint rpcBind(final int tcpPort, final RpcHandler handler) {
	    return rpcBind(tcpPort).rpcService(0, handler);
	}

	@Override
	synchronized public RpcFactory rpcBind(final int tcpPort) {
		return new RpcFactoryImpl(bind(tcpPort));
	}

	@Override
	synchronized public Serializer serializer() {
		if (serializer == null) {
			serializer = new KryoSerializer();
		}
		return serializer;
	}

	private Serializer serializer;
}
