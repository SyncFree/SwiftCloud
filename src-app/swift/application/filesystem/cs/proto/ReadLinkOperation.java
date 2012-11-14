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

import java.nio.CharBuffer;
import java.util.Arrays;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import fuse.FuseException;

public class ReadLinkOperation extends FuseRemoteOperation {

    String path;
    int linkCapacity;

    ReadLinkOperation() {
    }

    public ReadLinkOperation(String path, CharBuffer link) {
        this.path = path;
        this.linkCapacity = link.remaining();
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            CharBuffer tmp = CharBuffer.allocate(linkCapacity);
            int res = ((RemoteFuseOperationHandler) handler).readlink(path, tmp);
            handle.reply(new Result(res, tmp));
        } catch (FuseException e) {
            handle.reply(new FuseOperationResult());
        }
    }

    public static class Result extends FuseOperationResult {

        char[] data;

        Result() {
        }

        public Result(int ret, CharBuffer data) {
            super(ret);
            this.data = Arrays.copyOf(data.array(), data.position());
        }

        public void applyTo(CharBuffer dst) {
            dst.put(data);
        }
    }
}
