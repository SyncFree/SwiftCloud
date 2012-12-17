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
package sys.dht.catadupa;

import static sys.Sys.Sys;
import static sys.net.api.Networking.Networking;
import sys.RpcServices;
import sys.dht.catadupa.msgs.CatadupaHandler;
import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcFactory;

/**
 * 
 * @author smd
 * 
 */
public class LocalNode extends CatadupaHandler {

    protected Node self;
    protected RpcEndpoint rpc;
    protected Endpoint endpoint;
    protected RpcFactory rpcFactory;

    LocalNode() {
        initLocalNode();
    }

    public void initLocalNode() {
        rpcFactory = Networking.rpcBind(0, TransportProvider.DEFAULT);
        rpc = rpcFactory.toService(RpcServices.CATADUPA.ordinal(), this);
        self = new Node(rpc.localEndpoint(), Sys.getDatacenter());
        SeedDB.init(self);
    }
}
