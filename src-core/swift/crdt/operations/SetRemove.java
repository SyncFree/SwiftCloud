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
import swift.crdt.AbstractSetVersioned;

public class SetRemove<V, T extends AbstractSetVersioned<V, T>> extends BaseUpdate<T> {
    private V val;
    private Set<TripleTimestamp> ids;

    // required for kryo
    public SetRemove() {
    }

    public SetRemove(TripleTimestamp ts, V val, Set<TripleTimestamp> ids) {
        super(ts);
        this.val = val;
        this.ids = new HashSet<TripleTimestamp>();
        for (final TripleTimestamp id : ids) {
            this.ids.add(id.copyWithCleanedMappings());
        }
    }

    public V getVal() {
        return this.val;
    }

    public Set<TripleTimestamp> getIds() {
        return this.ids;
    }

    @Override
    public void applyTo(T crdt) {
        crdt.removeU(val, getTimestamp(), ids);
    }
}
