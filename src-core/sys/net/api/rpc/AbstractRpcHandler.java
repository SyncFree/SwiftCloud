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

/**
 * 
 * A convenience class for creating anonymous classes for processing invocations
 * involving specific message classes
 * 
 * @author smd
 * 
 */
abstract public class AbstractRpcHandler implements RpcHandler {

    @Override
    public void onFailure(final RpcHandle h) {
        Thread.dumpStack();
    }

    @Override
    public void onReceive(final RpcMessage m) {
        Thread.dumpStack();
    }

    @Override
    public void onReceive(final RpcHandle h, final RpcMessage m) {
        Thread.dumpStack();
    }
}
