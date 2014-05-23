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
import swift.crdt.SequenceCRDT.PosID;
import swift.crdt.core.CRDTUpdate;

public class SequenceInsertUpdate<V extends Comparable<V>> implements CRDTUpdate<SequenceCRDT<V>> {
    protected PosID<V> posId;
    protected TripleTimestamp ts;
    protected Set<TripleTimestamp> overwrittenInstances;

    // required for kryo
    SequenceInsertUpdate() {
    }

    public SequenceInsertUpdate(PosID<V> posId, TripleTimestamp ts, Set<TripleTimestamp> overwrittenInstances) {
        this.ts = ts;
        this.posId = posId;
        this.overwrittenInstances = overwrittenInstances;
    }

    @Override
    public void applyTo(SequenceCRDT<V> crdt) {
        crdt.applyAdd(posId, ts, overwrittenInstances);
    }

    @Override
    public Object getValueWithoutMetadata() {
        // TODO Auto-generated method stub
        return null;
    }
}
