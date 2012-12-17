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
package sys.dht.catadupa.crdts;

import java.util.Collection;
import java.util.Set;

import sys.dht.catadupa.crdts.time.Timestamp;

public abstract class AbstractORSet<T> implements Set<T> {

    protected CRDTRuntime rt = null;

    public AbstractORSet<T> setUpdatesRecorder(CRDTRuntime rt) {
        this.rt = rt;
        return this;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        boolean changed = false;
        for (T i : c)
            changed |= add(i);
        return changed;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object i : c)
            if (!contains(i))
                return false;
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean changed = false;
        for (Object i : c)
            changed |= remove(i);

        return changed;
    }

    @Override
    public Object[] toArray() {
        return toArray(new Object[size()]);
    }

    abstract public boolean add(T t, Timestamp ts);

}
