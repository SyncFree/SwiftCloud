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

import sys.dht.catadupa.Range;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class CatadupaCast implements RpcMessage {

    public int level;
    public Range range;
    public long rootKey;
    public CatadupaCastPayload payload;

    CatadupaCast() {
    }

    public CatadupaCast(final int level, final long rootKey, final Range range, final CatadupaCastPayload payload) {
        this.level = level;
        this.payload = payload;
        this.rootKey = rootKey;
        this.range = range.clone();
    }

    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((CatadupaHandler) handler).onReceive(handle, this);
    }
}
