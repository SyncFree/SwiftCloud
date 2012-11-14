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
package swift.crdt.operations;

import java.util.HashSet;
import java.util.Set;

import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.DirectoryVersioned;

public class DirectoryRemoveUpdate extends BaseUpdate<DirectoryVersioned> {
    private Set<TripleTimestamp> toBeRemoved;
    private CRDTIdentifier key;

    public DirectoryRemoveUpdate() {
        // Method stub for kryo
    }

    public DirectoryRemoveUpdate(CRDTIdentifier key, Set<TripleTimestamp> toBeRemoved, TripleTimestamp ts) {
        super(ts);
        this.key = key;
        this.toBeRemoved = new HashSet<TripleTimestamp>();
        for (final TripleTimestamp toBeRemovedTs : toBeRemoved) {
            this.toBeRemoved.add(toBeRemovedTs.copyWithCleanedMappings());
        }
    }

    @Override
    public void applyTo(DirectoryVersioned crdt) {
        crdt.applyRemove(key, toBeRemoved, getTimestamp());
    }

}
