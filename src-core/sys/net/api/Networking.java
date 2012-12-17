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
package sys.net.api;

import sys.net.api.rpc.RpcFactory;

/**
 * Used to obtain endpoints form performing message exchange.
 * 
 * @author smd
 * 
 */
abstract public class Networking {

    public enum TransportProvider {
        DEFAULT, NIO_TCP, NETTY_IO_TCP, NETTY_IO_WS
    }

    /**
     * Creates a local endpoint for accepting and sending messages
     * 
     * @param tcpPort
     *            the port used for listening tcp connections
     * @param handler
     *            the handler for processing incoming messages
     * @return the endpoint created
     */
    abstract public Endpoint bind(final int tcpPort, MessageHandler handler);

    /**
     * Creates a local endpoint for accepting and sending messages, using the
     * provider specified.
     * 
     * @param tcpPort
     *            the port used for listening for connections
     * @param provider
     *            the provider user for accepting connections
     * @param handler
     *            the handler for processing incoming messages
     * @return
     */
    abstract public Endpoint bind(final int tcpPort, TransportProvider provider, MessageHandler handler);

    /**
     * Creates an endpoint pointing to a remote location, identified by host/ip
     * and a port. Endpoints returned by this function serve only as locators
     * and cannot be used to initiate communication by themselves.
     * 
     * @param address
     *            - the host/ip[:port] address of the remote location
     * @param tcpPort
     *            - the port for establishing connections, ignored if host is
     *            specified as [host:port]
     * @return the endpoint created
     */
    abstract public Endpoint resolve(final String address, final int tcpPort);

    /**
     * Creates a local endpoint for accepting and sending messages according to
     * a simple RPC scheme. Allows for a sequence of cascading send/reply
     * message exchanges.
     * 
     * @param tcpPort
     *            the listening port
     * @param provider
     *            the provider user for accepting connections
     * @return
     */
    abstract public RpcFactory rpcBind(final int tcpPort, TransportProvider provider);

    /**
     * Creates a local endpoint for accepting and sending messages according to
     * a simple RPC scheme. Allows for a sequence of cascading send/reply
     * message exchanges.
     * 
     * @param tcpPort
     *            the listening port
     * @param provider
     *            the provider user for accepting connections
     * @return
     */
    abstract public RpcFactory rpcConnect(TransportProvider provider);

    /**
     * Creates a local endpoint form accepting and sending messages according to
     * a simple RPC scheme, using the default TCP provider. Allows for a
     * sequence of cascading send/reply message exchanges.
     * 
     * @param tcpPort
     *            the listening port
     * @return
     */
    abstract public RpcFactory rpcConnect();

    /**
     * Creates a rpc factory, which allows to register an handler associated
     * with numbered rpc services.
     * 
     * @param tcpPort
     *            the tcpPort used to send/receive messages for the rpc factory
     * @return
     */
    abstract public RpcFactory rpcBind(final int tcpPort);

    /**
     * Obtains a singleton instance of a serializer object
     * 
     * @return the serializer
     */
    abstract public Serializer serializer();

    protected Networking() {
        Networking = this;
    }

    /**
     * Sets the default transport provider...
     * 
     * @param provider
     *            - the default provider used for rpc endpoints...
     */
    abstract public void setDefaultProvider(TransportProvider provider);

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
