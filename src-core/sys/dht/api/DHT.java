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

    /**
     * Requests the DHT to resolve the endpoint of the DHT node responsible for
     * the given key.
     * 
     * @param key
     *            - the key to be resolved
     * @param timeout
     *            - the maximum amount of time in milliseconds to block for the
     *            answer.
     * @return the endpoint of the node rensponsible for the key, or null if no
     *         answer was received in the allowed timeout.
     */
    Endpoint resolveKey(final DHT.Key key, int timeout);

    void send(final DHT.Key key, final DHT.Message msg);

    void send(final DHT.Key key, final DHT.Message msg, int timeout);

    void send(final DHT.Key key, final DHT.Message msg, DHT.ReplyHandler handler);

    void send(final DHT.Key key, final DHT.Message msg, DHT.ReplyHandler handler, int timeout);

    interface Key {

        long longHashValue();
    }

    interface Message {

        void deliverTo(final DHT.Handle conn, final DHT.Key key, final DHT.MessageHandler handler);

    }

    interface MessageHandler {

        void onFailure();

        void onReceive(final DHT.Handle conn, final DHT.Key key, final DHT.Message request);

    }

    interface Reply {

        void deliverTo(final DHT.Handle conn, final DHT.ReplyHandler handler);

    }

    interface ReplyHandler {

        void onFailure();

        void onReceive(final DHT.Reply msg);

        void onReceive(final DHT.Handle conn, final DHT.Reply reply);
    }

    interface Handle {

        /**
         * Tells if this handle awaits a reply.
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
         * @return the endpoint of the node associated with the request...
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
        public void onReceive(DHT.Handle conn, DHT.Reply reply) {
        }
    }

    abstract class AbstractMessageHandler implements MessageHandler {

        @Override
        public void onFailure() {
        }

        @Override
        public void onReceive(DHT.Handle conn, DHT.Key key, DHT.Message request) {
        }
    }
}
