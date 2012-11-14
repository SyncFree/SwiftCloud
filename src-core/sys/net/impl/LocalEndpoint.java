/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package sys.net.impl;

import java.io.IOException;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.NetworkingException;
import sys.net.api.TransportConnection;

/**
 * Represents a local communication endpoint that listens for incoming messages
 * and allows establishing connections to remote endpoints for supporting
 * message exchanges
 * 
 * @author smd
 * 
 */
public class LocalEndpoint extends AbstractEndpoint {

	final AbstractLocalEndpoint provider;

	protected LocalEndpoint(int tcpPort, MessageHandler handler, TransportProvider providerType) throws Exception {
		super(handler);
		provider = getProvider( providerType, tcpPort ) ;
		provider.start();
		AbstractEndpoint.copyLocatorData(provider, this);
	}

	static synchronized Endpoint open(final int tcpPort, MessageHandler handler, TransportProvider provider) {
		try {
			return new LocalEndpoint(tcpPort, handler, provider);
		} catch (Exception x) {
		    x.printStackTrace();
			throw new NetworkingException(x);
		}
	}

	@Override
	public TransportConnection connect(Endpoint dst) {
		return provider.connect(dst);
	}

	@Override
	public TransportConnection send(Endpoint remote, Message m) {
		return provider.send(remote, m);
	}
	
	private AbstractLocalEndpoint getProvider( TransportProvider providerType, int port ) throws IOException {
		switch (providerType) {
		case NETTY_IO_WS:
			return new sys.net.impl.providers.netty.ws.WebSocketEndpoint(this, port);
		case NETTY_IO_TCP:
			return new sys.net.impl.providers.netty.tcp.TcpEndpoint(this, port);
		case NIO_TCP:
			return new sys.net.impl.providers.nio.TcpEndpoint(this, port);
		default:
			return getProvider( NetworkingImpl.defaultProvider, port);
		}
	}
}