package sys.net.impl;

import sys.net.api.Endpoint;
import sys.net.api.Networking;
import sys.net.api.NetworkingException;
import sys.net.api.Serializer;
import sys.net.api.rpc.RpcFactory;
import sys.net.impl.rpc.RpcFactoryImpl;

public class NetworkingImpl extends Networking {

	NetworkingImpl() {
	}

	public static void init() {
		new NetworkingImpl();
		KryoLib.init();
	}

	@Override
	synchronized public Endpoint bind(final int tcpPort) {
		return bind(tcpPort, TransportProvider.DEFAULT);
	}

	@Override
	synchronized public Endpoint bind(final int tcpPort, TransportProvider provider) {
		return LocalEndpoint.open(tcpPort, null, provider);
	}

	@Override
	public Endpoint resolve(final String host, final int tcpPort) {
		return new RemoteEndpoint(host, tcpPort);
	}

	@Override
	synchronized public RpcFactory rpcBind(final int tcpPort) {
		return rpcBind(tcpPort, TransportProvider.DEFAULT);
	}

	@Override
	synchronized public RpcFactory rpcBind(final int tcpPort, TransportProvider provider) {
		RpcFactoryImpl fac = new RpcFactoryImpl();
		fac.setEndpoint(LocalEndpoint.open(tcpPort, fac, provider));
		return fac;
	}

	@Override
	public RpcFactory rpcConnect(TransportProvider provider) {
		RpcFactoryImpl fac = new RpcFactoryImpl();
		fac.setEndpoint(LocalEndpoint.open(-1, fac, provider));
		return fac;
	}

	@Override
	public RpcFactory rpcConnect() {
		return rpcConnect(TransportProvider.DEFAULT);
	}

	@Override
	synchronized public Serializer serializer() {
		if (serializer == null) {
			serializer = new KryoSerializer();
		}
		return serializer;
	}

	private Serializer serializer;

	@Override
	public void setDefaultProvider(TransportProvider provider) {
		if( provider != TransportProvider.DEFAULT )
			defaultProvider = provider;
		else
			throw new NetworkingException("Invalid argument...");
	}

	static TransportProvider defaultProvider = TransportProvider.NIO_TCP ;
}
