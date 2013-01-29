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
package sys.net.api.rpc;

import sys.net.api.Endpoint;

/**
 * 
 * Represents a local endpoint listening for incoming rpc messages.
 * 
 * @author smd
 * 
 */
public interface RpcEndpoint {

    /**
     * The local endpoint information (ip + tcp port)
     * 
     * @return
     */
    Endpoint localEndpoint();

    /**
     * Sends a request message to a destination endpoint, waiting for a reply.
     * 
     * @param dst
     *            - the endpoint that will receive the message request
     * @param m
     *            - the message that defines the request.
     * @return the reply to the request, null if a timeout ocurred.
     */
    <T extends RpcMessage> T request(final Endpoint dst, final RpcMessage m);

    /**
     * 
     * Sends an invocation message to a (listening) destination endpoint
     * 
     * @param dst
     *            the destination of the invocation message, blocking until the
     *            message is written to the underlying channel.
     * @param m
     *            the message being sent
     * @return the handle associated for the message
     */
    RpcHandle send(final Endpoint dst, final RpcMessage m);

    /**
     * Sends an invocation message to a (listening) destination endpoint,
     * blocking until a reply is received (or the default timeout expires).
     * 
     * @param dst
     *            the destination of the invocation message
     * @param m
     *            the message being sent
     * @param replyHandler
     *            - the handler for processing the reply message
     * @return the handle associated for the message
     */
    RpcHandle send(final Endpoint dst, final RpcMessage m, final RpcHandler replyHandler);

    /**
     * Sends an invocation message to a (listening) destination endpoint,
     * blocking until a reply is received or the timeout expires.
     * 
     * @param dst
     *            the destination of the invocation message
     * @param m
     *            the message being sent
     * @param replyHandler
     *            - the handler for processing the reply message
     * 
     * @param timout
     *            - number of milliseconds to wait for the reply. <= 0 returns
     *            immediately. FIXME: document MAX_TIMEOUT
     * @return the handle associated for the message
     */
    RpcHandle send(final Endpoint dst, final RpcMessage m, final RpcHandler replyHandler, int timeout);

    /**
     * Sets the handler responsible for processing incoming invocation messages
     * 
     * @param handler
     *            the handler for processing invocation messages
     * @return itself
     */
    <T extends RpcEndpoint> T setHandler(final RpcHandler handler);

    /**
     * Obtains a reference to the RPC factory used to obtain this endpoint.
     * 
     * @return the reference to the factory that created this endpoint.
     */
    RpcFactory getFactory();

    /**
     * Sets the default timeout for this endpoint, while waiting for a reply to
     * a message sent.
     * 
     * @param ms
     *            - the new timeout value to be used in milliseconds.
     */
    void setDefaultTimeout(int ms);

    /**
     * Obtains the default timeout in use for this endpoint, while waiting for a
     * reply to a message sent.
     * 
     * @return the timeout in milliseconds.
     */
    int getDefaultTimeout();

    /**
     * Sets options ... TODO document this...
     * 
     * @param option
     * @param val
     */
    void setOption(String option, Object val);
}
