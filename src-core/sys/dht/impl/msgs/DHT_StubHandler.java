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
package sys.dht.impl.msgs;

import sys.net.api.rpc.AbstractRpcHandler;
import sys.net.api.rpc.RpcHandle;

abstract public class DHT_StubHandler extends AbstractRpcHandler {

	@Override
	public void onFailure(final RpcHandle handle) {
		Thread.dumpStack();
	}

	public void onReceive(final RpcHandle conn, final DHT_ResolveKey req) {
		Thread.dumpStack();
	}

	public void onReceive(final RpcHandle conn, final DHT_ResolveKeyReply rep) {
		Thread.dumpStack();
	}

	public void onReceive(final RpcHandle conn, final DHT_Request req) {
		Thread.dumpStack();
	}

	public void onReceive(final RpcHandle conn, final DHT_RequestReply reply) {
		Thread.dumpStack();
	}

	public void onReceive(final RpcHandle conn, final DHT_ReplyReply reply) {
		Thread.dumpStack();
	}

}
