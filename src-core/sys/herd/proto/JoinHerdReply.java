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

import java.util.HashMap;
import java.util.Map;

import sys.herd.Herd;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class JoinHerdReply implements RpcMessage {

    long age;
    Map<String, Map<String, Herd>> herds;

    public JoinHerdReply() {
    }

    public JoinHerdReply(long age, Map<String, Map<String, Herd>> herds) {
        this.age = age;
        this.herds = new HashMap<String, Map<String, Herd>>();
        for (Map.Entry<String, Map<String, Herd>> i : herds.entrySet()) {

            Map<String, Herd> copy = new HashMap<String, Herd>();
            for (Map.Entry<String, Herd> j : i.getValue().entrySet())
                copy.put(j.getKey(), j.getValue());

            this.herds.put(i.getKey(), copy);
        }
    }

    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        ((HerdProtoHandler) handler).onReceive(this);
    }

    public Map<String, Map<String, Herd>> herds() {
        return herds;
    }

    public long age() {
        return age;
    }
}
