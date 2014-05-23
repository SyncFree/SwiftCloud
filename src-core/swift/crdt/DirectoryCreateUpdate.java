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
package swift.crdt;

import java.util.Set;

import swift.clocks.TripleTimestamp;
import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.CRDTUpdate;

public class DirectoryCreateUpdate implements CRDTUpdate<DirectoryCRDT> {
    protected CRDTIdentifier entry;
    protected TripleTimestamp ts;
    private Set<TripleTimestamp> overwrittenTimestamps;

    public DirectoryCreateUpdate() {
        // method stub for kryo...
    }

    public DirectoryCreateUpdate(CRDTIdentifier val, TripleTimestamp ts, Set<TripleTimestamp> overwrittenTimestamps) {
        // TODO Check that key and type of CRDT are consistent
        this.ts = ts;
        this.entry = val;
        this.overwrittenTimestamps = overwrittenTimestamps;
    }

    @Override
    public void applyTo(DirectoryCRDT crdt) {
        crdt.applyCreate(entry, ts, overwrittenTimestamps);
    }

    @Override
    public Object getValueWithoutMetadata() {
        return entry;
    }
}
