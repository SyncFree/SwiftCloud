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
package swift.dc.db;

import java.util.Properties;

import swift.crdt.CRDTIdentifier;
import swift.dc.CRDTData;

/**
 * Database that stores no information
 * @author nmp
 *
 */
public class DevNullNodeDatabase implements DCNodeDatabase {

    @Override
    public void init(Properties props) {
        
    }

    @Override
    public CRDTData<?> read(CRDTIdentifier id) {
        return null;
    }

    @Override
    public boolean write(CRDTIdentifier id, CRDTData<?> data) {
        return true;
    }

    @Override
    public boolean ramOnly() {
        return true;
    }

    @Override
    public Object readSysData(String table, String key) {
        return null;
    }

    @Override
    public boolean writeSysData(String table, String key, Object data) {
        return true;
    }
}
