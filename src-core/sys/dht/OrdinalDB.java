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
package sys.dht;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import sys.herd.Herd;
import sys.net.api.Endpoint;

/**
 * 
 * @author smd
 * 
 */
public class OrdinalDB {

    Node self;
    SortedMap<Long, Node> k2n = new TreeMap<Long, Node>();

    public OrdinalDB populate(Herd herd, Endpoint endpoint) {
        k2n.clear();

        int total = herd.sheep().size();

        SortedMap<Long, Node> tmp = new TreeMap<Long, Node>();

        for (Endpoint i : herd.sheep()) {
            Node n = new Node(i, herd.dc());
            tmp.put(n.key, n);
        }

        int order = 0;
        for (Node i : tmp.values()) {
            long key = (Node.MAX_KEY / total) * order++;
            k2n.put(key, new Node(key, i.endpoint));
        }

        for (Node i : k2n.values())
            if (i.endpoint.equals(endpoint)) {
                self = i;
                break;
            }

        return this;
    }

    public Node self() {
        return self;
    }

    public Set<Long> nodeKeys() {
        return k2n.keySet();
    }

    public Collection<Node> nodes() {
        return k2n.values();
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
