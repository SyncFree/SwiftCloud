package swift.crdt;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.SortedSetInsert;
import swift.crdt.operations.SortedSetRemove;

public class SortedSetTxnLocal<V extends Comparable<V>> extends BaseCRDTTxnLocal<SortedSetVersioned<V>> {

	private SortedMap<V, Set<TripleTimestamp>> elems;

	public SortedSetTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, SortedSetVersioned<V> creationState, SortedMap<V, Set<TripleTimestamp>> elems) {
		super(id, txn, clock, creationState);
		this.elems = elems;
	}

	@Override
	public Set<V> getValue() {
		return Collections.unmodifiableSet( elems.keySet() );
	}

	public int size() {
	    return elems.size();
	}
	/**
	 * Insert element e in the supporting sorted set, using the given unique
	 * identifier.
	 * 
	 * @param e
	 */
	public void insert(V v) {
		Set<TripleTimestamp> adds = elems.get( v );
		if (adds == null) {
			adds = new HashSet<TripleTimestamp>();
			elems.put(v, adds);
		}
		TripleTimestamp ts = nextTimestamp();
		adds.add( ts );
		registerLocalOperation(new SortedSetInsert<V, SortedSetVersioned<V>>( ts, v ));
	}

	/**
	 * Remove element e from the supporting sorted set.
	 * 
	 * @param e
	 */
	public V remove(V v) {
		Set<TripleTimestamp> ids = elems.remove(v);
		if (ids != null) {
			TripleTimestamp ts = nextTimestamp();
			registerLocalOperation(new SortedSetRemove<V, SortedSetVersioned<V>>( ts, v, ids));
	        return v;
		} else
		    return null;
	}

    @Override
    public Object executeQuery(CRDTQuery<SortedSetVersioned<V>> query) {
        return query.executeAt(this);
    }

  
	
}
