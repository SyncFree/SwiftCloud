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
package sys.shepard.proto;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class ShepardProtoHandler implements RpcHandler {

    @Override
    public void onReceive(RpcMessage m) {
    }

    @Override
    public void onReceive(RpcHandle handle, RpcMessage m) {
    }

    @Override
    public void onFailure(RpcHandle handle) {
    }

    public void onReceive(RpcHandle client, GrazingRequest q) {
    }

    public void onReceive(GrazingGranted p) {
    }

    public void onReceive(GrazingAccepted p) {
        System.err.println("Request Accepted from Shepard!!!");
    }
}
