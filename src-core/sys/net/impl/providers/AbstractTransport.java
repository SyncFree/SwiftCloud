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
package sys.net.impl.providers;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.NetworkingException;
import sys.net.api.TransportConnection;

public abstract class AbstractTransport implements TransportConnection {

    protected Endpoint remote;
    protected boolean isBroken;
    protected final Endpoint local;

    public AbstractTransport(Endpoint local, Endpoint remote) {
        this.local = local;
        this.remote = remote;
        this.isBroken = false;
    }

    @Override
    public void dispose() {
    }

    @Override
    public boolean send(Message m) {
        throw new NetworkingException("Invalid connection state...");
    }

    @Override
    public <T extends Message> T receive() {
        throw new NetworkingException("Invalid connection state...");
    }

    @Override
    public boolean failed() {
        return isBroken;
    }

    @Override
    public Endpoint localEndpoint() {
        return local;
    }

    @Override
    public Endpoint remoteEndpoint() {
        return remote;
    }

    public void setOption(String op, Object val) {
    }

    public void setRemoteEndpoint(Endpoint remote) {
        this.remote = remote;
    }

}