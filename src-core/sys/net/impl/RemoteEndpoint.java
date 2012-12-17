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

import java.net.InetSocketAddress;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.NetworkingException;
import sys.net.api.TransportConnection;

import com.esotericsoftware.kryo.KryoSerializable;

/**
 * 
 * Represents a remote endpoint location. Used as a destination for sending
 * messages.
 * 
 * @author smd
 * 
 */
public class RemoteEndpoint extends AbstractEndpoint implements KryoSerializable {

    private int delay = 0;
    private long connectionAttempt = 0;

    public RemoteEndpoint() {
    }

    public RemoteEndpoint(final String host, final int tcpPort) {
        super(new InetSocketAddress(host, tcpPort), 0);
    }

    public RemoteEndpoint(long locator, long gid) {
        super(locator, gid);
    }

    @Override
    public <T extends Endpoint> T setHandler(MessageHandler handler) {
        throw new NetworkingException("Not supported...[This is a remote endpoint...]");
    }

    @Override
    public TransportConnection send(final Endpoint dst, final Message m) {
        throw new NetworkingException("Not supported...[This is a remote endpoint...]");
    }

    @Override
    public TransportConnection connect(Endpoint dst) {
        throw new NetworkingException("Not supported...[This is a remote endpoint...]");
    }

    public int getConnectionRetryDelay() {
        return 10;
        // long elapsed = System.currentTimeMillis() - connectionAttempt;
        // if( elapsed > 2000) {
        // connectionAttempt += elapsed;
        // delay = 0;
        // }
        // delay += 2 * (delay + 1);
        // return delay;
    }
}
