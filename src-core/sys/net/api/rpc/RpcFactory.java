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

public interface RpcFactory {

	/**
	 * Creates a service for accepting and sending messages according to a
	 * simple RPC scheme. Allows for a sequence of cascading send/reply message
	 * exchanges.
	 * 
	 * @param service
	 *            the id of the service that will send/receive messages
	 * @param handler
	 *            the handler used for message delivery for this service
	 * @return the endpoint that is bound to the service
	 */
	RpcEndpoint toService(int service, RpcHandler handler);

	/**
	 * Creates a connection to service for accepting and sending messages
	 * according to a simple RPC scheme. Allows for a sequence of cascading
	 * send/reply message exchanges.
	 * 
	 * @param service
	 *            the id of the service that will send/receive messages
	 * @param handler
	 *            the handler used for message delivery for this service
	 * @return the endpoint that is bound to the service
	 */
	RpcEndpoint toService(int service);

	/**
	 * Creates a connection to service for accepting and sending messages
	 * according to a simple RPC scheme. Allows for a sequence of cascading
	 * send/reply message exchanges.
	 * 
	 * @param service
	 *            the id of the service that will send/receive messages
	 * @param handler
	 *            the handler used for message delivery for this service
	 * @return the endpoint that is bound to the default service (zero)
	 * 
	 */
	RpcEndpoint toDefaultService();

}
