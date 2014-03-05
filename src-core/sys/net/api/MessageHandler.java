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
 * 
 * The generic handler called to received incoming messages. This interface is
 * meant to be extended with specific onReceive methods for the expected
 * incoming messages. It also requires that messages implement the deliverTo
 * method, accordingly.
 * 
 * @author smd
 * 
 */
public interface MessageHandler {

    /**
     * Reports the establishment of a new incoming connection to some endpoint
     * 
     * @param conn
     *            the new incoming connection
     */
    void onAccept(final TransportConnection conn);

    /**
     * Reports the establishment of a new outgoing connection to some endpoint
     * 
     * @param conn
     *            the new outgoing connection
     */
    void onConnect(final TransportConnection conn);

    /**
     * Reports the failure of a connection to some endpoint
     * 
     * @param conn
     *            the connection that failed
     */
    void onFailure(final TransportConnection conn);

    /**
     * Reports the "normal" closure of a connection to some endpoint
     * 
     * @param conn
     *            the connection that was closed.
     */
    void onClose(final TransportConnection conn);

    /**
     * Called whenever a connection to a remote endpoint cannot be established
     * or fails.
     * 
     * @param dst
     *            - the remote endpoint
     * @param m
     *            - the message that was being sent
     */
    void onFailure(final Endpoint dst, final Message m);

    /**
     * Called to upon the arrival of a message in the given connection.
     * 
     * @param conn
     *            the connection that received the message, which may be used to
     *            send a reply back.
     * @param m
     *            the message received in the connection
     */
    void onReceive(final TransportConnection conn, final Message m);
}
