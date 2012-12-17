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
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.TransportConnection;

public abstract class AbstractLocalEndpoint extends AbstractEndpoint {

    protected Endpoint localEndpoint;

    abstract public void start() throws Exception;

    abstract public TransportConnection connect(Endpoint dst);

    @Override
    public TransportConnection send(Endpoint remote, Message m) {
        TransportConnection conn = connect(remote);
        if (conn != null && conn.send(m))
            return conn;
        else
            return null;
    }

    protected long getLocator(SocketAddress addr) {
        InetSocketAddress iaddr = (InetSocketAddress) addr;
        return ByteBuffer.wrap(iaddr.getAddress().getAddress()).getInt() << 32 | iaddr.getPort();
    }
}
