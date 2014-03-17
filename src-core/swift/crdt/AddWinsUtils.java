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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import swift.clocks.TripleTimestamp;

/**
 * Helper class with static methods for processing common type of payload. The
 * methods are used to reduce code duplication and simplify the maintenance.
 * 
 * @author annettebieniusa, mzawirsk
 * 
 */
public final class AddWinsUtils {
    // 4 is a magic constant secretly shared with me by aliens...
    // ... which works well for the common case of singleton set.
    public static final int SMALL_HASHSET_EXPECTED_CAPACITY = 4;

    public static <V> Set<TripleTimestamp> add(Map<V, Set<TripleTimestamp>> elemsInstances, V element,
            final TripleTimestamp newInstance) {
        final HashSet<TripleTimestamp> newInstances = new HashSet<TripleTimestamp>(SMALL_HASHSET_EXPECTED_CAPACITY);
        newInstances.add(newInstance);
        return elemsInstances.put(element, newInstances);
    }

    public static <V> void applyAdd(Map<V, Set<TripleTimestamp>> elemsInstances, V element, TripleTimestamp instance,
            Collection<?> overwrittenInstances) {
        Set<TripleTimestamp> instances = elemsInstances.get(element);
        if (instances == null) {
            instances = new HashSet<TripleTimestamp>(SMALL_HASHSET_EXPECTED_CAPACITY);
            elemsInstances.put(element, instances);
        } else if (overwrittenInstances != null) {
            // Self-cleaning GC.
            instances.removeAll(overwrittenInstances);
        }
        instances.add(instance);
    }

    public static <V> Set<TripleTimestamp> remove(Map<V, Set<TripleTimestamp>> elemsInstances, V element) {
        return elemsInstances.remove(element);
    }

    public static <V> void applyRemove(Map<V, Set<TripleTimestamp>> elemsInstances, V element,
            Set<TripleTimestamp> removedInstances) {
        Set<TripleTimestamp> instances = elemsInstances.get(element);
        if (instances == null) {
            return;
        }
        instances.removeAll(removedInstances);
        if (instances.isEmpty()) {
            elemsInstances.remove(element);
        }
    }

    public static <V, T extends Map<V, Set<TripleTimestamp>>> void deepCopy(T origInstances, T newInstances) {
        for (final Entry<V, Set<TripleTimestamp>> entry : origInstances.entrySet()) {
            newInstances.put(entry.getKey(), new HashSet<TripleTimestamp>(entry.getValue()));
        }
    }
}
