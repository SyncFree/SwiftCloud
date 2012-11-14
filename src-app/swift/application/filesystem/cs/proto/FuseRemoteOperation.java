/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2012 Kaiserslautern University of Technology
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
package swift.application.filesystem.cs.proto;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class FuseRemoteOperation implements RpcMessage {

    //for kryo
    FuseRemoteOperation() {    
    }
    
    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((RemoteFuseOperationHandler)handler).onReceive(handle, this);
    }

}
