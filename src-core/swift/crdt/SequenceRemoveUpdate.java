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

public class SequenceRemoveUpdate<V extends Comparable<V>> implements CRDTUpdate<SequenceCRDT<V>> {
    protected Set<TripleTimestamp> ids;
    protected PosID<V> posId;

    // required for kryo
    SequenceRemoveUpdate() {
    }

    public SequenceRemoveUpdate(PosID<V> posId, Set<TripleTimestamp> ids) {
        this.posId = posId;
        this.ids = ids;
    }

    @Override
    public void applyTo(SequenceCRDT<V> crdt) {
        crdt.applyRemove(posId, ids);
    }
}
