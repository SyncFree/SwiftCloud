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
package swift.dc;

import swift.crdt.CRDTIdentifier;
import swift.proto.ObjectUpdatesInfo;

/**
 * Result of an exec operation in a CRDT
 * 
 * @author preguica
 * 
 */
public class ExecCRDTResult {

    boolean result;
    CRDTIdentifier id;
    ObjectUpdatesInfo info;

    public ExecCRDTResult(boolean result) {
        this.info = null;
        this.result = result;
    }

    public ExecCRDTResult(boolean result, CRDTIdentifier id, ObjectUpdatesInfo info) {
        this.id = id;
        this.info = info;
        this.result = result;
    }

    /**
     * Needed for Kryo serialization
     */
    ExecCRDTResult() {
    }

    public boolean isResult() {
        return result;
    }

    public ObjectUpdatesInfo getInfo() {
        return info;
    }

    public CRDTIdentifier getId() {
        return id;
    }
}
