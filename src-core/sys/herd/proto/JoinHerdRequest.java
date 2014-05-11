/*****************************************************************************
 * Copyright 2011-2014 INRIA
 * Copyright 2011-2014 Universidade Nova de Lisboa
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
package sys.herd.proto;

import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class JoinHerdRequest implements RpcMessage {

    String dc;
    String herd;
    Endpoint endpoint;

    public JoinHerdRequest() {
    }

    public JoinHerdRequest(String dc, String herd, Endpoint endpoint) {
        this.dc = dc;
        this.herd = herd;
        this.endpoint = endpoint;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((HerdProtoHandler) handler).onReceive(handle, this);
    }

    public String dc() {
        return dc;
    }

    public String herd() {
        return herd;
    }

    public Endpoint sheep() {
        return endpoint;
    }
}
