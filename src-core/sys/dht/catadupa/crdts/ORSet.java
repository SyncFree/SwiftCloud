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


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.logging.Logger;

import sys.dht.catadupa.crdts.time.Timestamp;

public class ORSet<V> extends AbstractORSet<V> implements CvRDT<ORSet<V>> {

	private static Logger Log = Logger.getLogger("sys.dht.catadupa");
	
	Set<Timestamp> tomb;
	Map<V, Set<Timestamp>> e2t;
	Map<Timestamp, V> t2v;

	public ORSet() {
		e2t = new HashMap<V, Set<Timestamp>>();
		t2v = new HashMap<Timestamp, V>();
		tomb = new HashSet<Timestamp>();
	}

	@Override
	public boolean isEmpty() {
		return e2t.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return e2t.containsKey(o);
	}

	@Override
	public int size() {
		return e2t.size();
	}

	@Override
	public boolean add(V v) {
		return add(v, rt.recordUpdate(this));
	}

	@Override
	public boolean add(V v, Timestamp t) {
		t2v.put( t, v );
		return get(v).add(t);
	}

	@Override
	public boolean remove(Object o) {
		Set<Timestamp> s = e2t.remove(o);
		if (s != null) {
			tomb.addAll(s);
			t2v.keySet().removeAll(s);
		}
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		Set<V> deletes = new HashSet<V>(e2t.keySet());
		deletes.removeAll(c);
		for (V i : deletes)
			remove(i);

		return deletes.size() > 0;
	}

	@Override
	public synchronized void clear() {
		for (Entry<V, Set<Timestamp>> i : e2t.entrySet()) {
			tomb.addAll(i.getValue());
		}
		e2t.clear();
		t2v.clear();
	}

	@Override
	public Iterator<V> iterator() {
		return new _OrSetIterator();
	}

	@Override
	public <Q> Q[] toArray(Q[] a) {
		return e2t.keySet().toArray(a);
	}

	@Override
	public void merge(ORSet<V> other) {
		merge(other, new ArrayList<V>(), new ArrayList<V>());
	}

	public synchronized void merge(ORSet<V> other, Collection<V> added, Collection<V> removed) {

		List<Timestamp> newTombs = new ArrayList<Timestamp>();
		for (Timestamp t : other.tomb)
			if (tomb.add(t))
				newTombs.add(t);

		for (Map.Entry<V, Set<Timestamp>> e : other.e2t.entrySet()) {

			V v = e.getKey();
			for (Timestamp t : e.getValue())
				if (!tomb.contains(t)) {
					if( get(v).add(t) )
						added.add(v);
					t2v.put( t, v);
				}
		}

		for (Timestamp t : newTombs) {
			V v = t2v.remove(t);
			if( v != null )
				removed.add(v);
		}
		
		Log.finest("Added:" + added + " " + "Removed:" + removed);
	}

	public Map<V, Timestamp> subSet(Collection<? extends Timestamp> timestamps) {
		Map<V, Timestamp> res = new HashMap<V, Timestamp>();
		for (Timestamp t : timestamps) {
			V v = t2v.get(t);
			if( v != null )
				res.put( v, t);
		}
		return res;
	}

	private Set<Timestamp> get(V v) {
		Set<Timestamp> s = e2t.get(v);
		if (s == null)
			e2t.put(v, s = new HashSet<Timestamp>());
		return s;
	}

//	private Set<V> get(Timestamp t) {
//		Set<V> s = t2e.get(t);
//		if (s == null)
//			t2e.put(t, s = new HashSet<V>());
//		return s;
//	}

	@Override
	public String toString() {
		return e2t.keySet().toString();
	}

	class _OrSetIterator implements Iterator<V> {

		Map.Entry<V, Set<Timestamp>> curr;
		Iterator<Map.Entry<V, Set<Timestamp>>> it;

		_OrSetIterator() {
			it = e2t.entrySet().iterator();
		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public V next() {
			return (curr = it.next()).getKey();
		}

		@Override
		public void remove() {
			tomb.addAll(curr.getValue());
			t2v.entrySet().remove(curr.getValue());
			it.remove();
		}
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
}
