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
package sys.dht.test;

import sys.dht.api.DHT;
import sys.dht.test.msgs.StoreData;
import sys.dht.test.msgs.StoreDataReply;

/**
 * 
 * The KVS interface used in this example, split into its client/server sides...
 * 
 * @author smd
 * 
 */
public interface KVS {

	/**
	 * Denotes collection of requests/messages that the KVS server
	 * processes/expects
	 * 
	 * @author smd
	 * 
	 */
	abstract class RequestHandler extends DHT.AbstractMessageHandler {
		abstract public void onReceive(DHT.Handle handle, DHT.Key key, StoreData request);
	}

	/**
	 * Denotes collection of reply/messages that the KVS client processes
	 * 
	 * @author smd
	 * 
	 */
	abstract class ReplyHandler extends DHT.AbstractReplyHandler {
		abstract public void onReceive(StoreDataReply reply);
	}
}
