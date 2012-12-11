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

import java.nio.ByteBuffer;
import java.util.Arrays;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import fuse.FuseException;

public class GetXAttrOperation extends FuseRemoteOperation {

    String path;
    String name;
    int dstCapacity; 
    int position;
    
    GetXAttrOperation() {        
    }
    
    public GetXAttrOperation(String path, String name, ByteBuffer dst, int position) {
        this.path = path;
        this.name = name;
        this.dstCapacity = dst.remaining();
        this.position = position;
    }
    
    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            ByteBuffer tmp = ByteBuffer.allocate( dstCapacity );
            int res = ((RemoteFuseOperationHandler)handler).getxattr(path, name, tmp, position);
            handle.reply( new GetXAttrOperationResult( res, tmp ) ) ;
        } catch (FuseException e) {
            handle.reply( new FuseOperationResult() );
        }
    }
    
    static class GetXAttrOperationResult extends FuseOperationResult {
        
        byte[] data;
        
        GetXAttrOperationResult() {            
        }
        
        public GetXAttrOperationResult( int ret, ByteBuffer data ) {
            super( ret ) ;
            this.data = Arrays.copyOf( data.array(), data.position() ) ;
        }
        
        public void applyTo( ByteBuffer dst ) {
            dst.put( data ) ;
        }
    }
}
