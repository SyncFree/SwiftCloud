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
package sys.dht.catadupa;

import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 
 * @author smd
 * 
 */
public class OrdinalDB {

    final long MAX_KEY = DB.MAX_KEY;

    final DB db;
    SortedMap<Long, Node> k2n = new TreeMap<Long, Node>();

    public OrdinalDB(DB db) {
        this.db = db;
    }

    void populate() {
        k2n.clear();
        int t = db.nodeKeys().size();

        int o = 0;
        for (Node j : db.nodes(0L)) {
            long key = (MAX_KEY / t) * o++;
            k2n.put(key, new Node(key, j.endpoint));
        }
    }

    public Set<Long> nodeKeys() {
        return k2n.keySet();
    }

    public Iterable<Node> nodes(long key) {
        Iterator<Node> first = k2n.tailMap(key).values().iterator();
        Iterator<Node> second = k2n.headMap(key).values().iterator();
        return new AppendIterator<Node>(first, second);
    }

    class AppendIterator<T> implements Iterator<T>, Iterable<T> {

        Iterator<T> curr, first, second;

        AppendIterator(Iterator<T> a, Iterator<T> b) {
            curr = first = a;
            second = b;
        }

        @Override
        public boolean hasNext() {
            if (curr.hasNext())
                return true;
            else
                return (curr = second).hasNext();
        }

        @Override
        public T next() {
            return curr.next();
        }

        @Override
        public void remove() {
            curr.remove();
        }

        @Override
        public Iterator<T> iterator() {
            return this;
        }
    }
}
