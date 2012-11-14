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
package sys.dht.catadupa.msgs;

import java.util.Map;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

import sys.dht.catadupa.MembershipUpdate;
import sys.dht.catadupa.crdts.ORSet;
import sys.dht.catadupa.crdts.time.LVV;
import sys.dht.catadupa.crdts.time.Timestamp;

public class DbMergeReply implements RpcMessage {

	public LVV clock;
	public Map<MembershipUpdate, Timestamp> delta;

	DbMergeReply() {
	}

	public DbMergeReply(LVV clock, Map<MembershipUpdate, Timestamp> delta) {
		this.clock = clock;
		this.delta = delta;
	}

	public ORSet<MembershipUpdate> toORSet() {
		ORSet<MembershipUpdate> res = new ORSet<MembershipUpdate>();
		for (Map.Entry<MembershipUpdate, Timestamp> i : delta.entrySet())
			res.add(i.getKey(), i.getValue());
		return res;
	}

	public void deliverTo(RpcHandle handle, RpcHandler handler) {
		if (handle.expectingReply())
			((CatadupaHandler) handler).onReceive(handle, this);
		else
			((CatadupaHandler) handler).onReceive(this);
	}
}
