/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2012 University of Kaiserslautern
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

import swift.application.filesystem.cs.SwiftFuseServer;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import fuse.FuseException;
import fuse.FuseOpenSetter;

public class OpenOperation extends FuseRemoteOperation {

    String path;
    int flags;

    OpenOperation() {
    }

    public OpenOperation(String path, int flags, FuseOpenSetter openSetter) {
        this.path = path;
        this.flags = flags;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            Result result = new Result();
            int res = ((RemoteFuseOperationHandler) handler).open(path, flags, result);
            result.setResult(res);
            handle.reply(result);
        } catch (FuseException e) {
            e.printStackTrace();
            handle.reply(new FuseOperationResult());
        }
    }

    public static class Result extends FuseOperationResult implements FuseOpenSetter {

        Object fh;
        boolean isDirectIO;
        boolean keepInCache;

        Result() {
        }

        public void applyTo(FuseOpenSetter setter) {
            setter.setFh(fh);
            setter.setDirectIO(isDirectIO);
            setter.setKeepCache(keepInCache);
        }

        public void setResult(int result) {
            super.result = result;
        }

        @Override
        public boolean isDirectIO() {
            return isDirectIO;
        }

        @Override
        public boolean isKeepCache() {
            return keepInCache;
        }

        @Override
        public void setDirectIO(boolean directIO) {
            this.isDirectIO = directIO;
        }

        @Override
        public void setFh(Object fh) {
            this.fh = SwiftFuseServer.s2c_fh(fh);
        }

        @Override
        public void setKeepCache(boolean keepInCache) {
            this.keepInCache = keepInCache;
            Thread.dumpStack();
        }

        public String toString() {
            return String.format("%s (%s, %s, %s, %s)", getClass(), result, fh, isDirectIO, keepInCache);
        }
    }

}
