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

/**
 * Represents a connection to a remote endpoint, which can be used to exchange
 * messages.
 * 
 * @author smd
 * 
 */
public interface TransportConnection {

    /**
     * Obtains the state of the connection. Connections may fail to be
     * established or fail during message exchange.
     * 
     * @return true if the connection has failed to be established or has failed
     *         since; false otherwise
     */
    boolean failed();

    /**
     * Disposes of this connection.
     */
    void dispose();

    /**
     * Sends a message using this connection
     * 
     * @param m
     *            the message being sent
     * @return false if an error occurred; true if no error occurred.
     */
    boolean send(final Message m);

    /**
     * Obtains the local endpoint for this connection
     * 
     * @return the local endpoint associated with this connection
     */
    Endpoint localEndpoint();

    /**
     * Obtains the remote endpoint for this connection
     * 
     * @return the remote endpoint associated with this connection
     */
    Endpoint remoteEndpoint();

    /**
     * If the connection has failed due to an exception, this method provides
     * the cause of failure.
     * 
     * @return the cause for the connection failure.
     */
    Throwable causeOfFailure();

    /**
     * Allows low-level configuration of the connection socket implementation,
     * such as the thread executor used for dispatching incoming messages
     */
    void setOption(String op, Object value);
}