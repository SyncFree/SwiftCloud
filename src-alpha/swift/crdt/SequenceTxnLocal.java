package swift.crdt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.operations.SequenceInsert;
import swift.crdt.operations.SequenceRemove;

import static swift.crdt.SequenceVersioned.*;

public class SequenceTxnLocal<V> extends BaseCRDTTxnLocal<SequenceVersioned<V>> {

	private SortedMap<PosID<V>, Set<TripleTimestamp>> elems;
	private List<V> atoms = new ArrayList<V>();
	private List<SID> posIDs = new ArrayList<SID>();

	public SequenceTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, SequenceVersioned<V> creationState, SortedMap<PosID<V>, Set<TripleTimestamp>> elems) {
		super(id, txn, clock, creationState);
		this.elems = elems;
		this.linearize();
	}

	/**
	 * Inserts atom d into the position pos of the sequence
	 */
	public void insertAt(int pos, V v) {
		SID id = newId(pos);		
		insert(new PosID<V>(id, v));
		atoms.add(pos, v);
		posIDs.add(pos, id);
	}

	/**
	 * Deletes atom at position pos
	 */
	public void removeAt(int pos) {
		SID id = posIDs.remove(pos);
		V v = atoms.remove(pos);
		for (Map.Entry<PosID<V>, Set<TripleTimestamp>> i : elems.entrySet()) {
			PosID<V> k = i.getKey();
			if (id.equals(k.getId())) {
				remove(k);
				break;
			}
		}
	}
	
	public int size() {
		return atoms.size();
	}

	@Override
	public Object executeQuery(CRDTQuery<SequenceVersioned<V>> query) {
		return query.executeAt(this);
	}

	@Override
	public List<V> getValue() {
		return Collections.unmodifiableList(atoms);
	}

	private void linearize() {
		atoms.clear();
		posIDs.clear();
		for (Map.Entry<PosID<V>, Set<TripleTimestamp>> i : elems.entrySet()) {
			PosID<V> k = i.getKey();
			if (!k.isDeleted()) {
				atoms.add(k.getAtom());
				posIDs.add(k.getId());
			}
		}
	}

	final private SID newId(int pos) {		
		if (pos == 0)
			return size() == 0 ? SID.FIRST : SID.smallerThan(posIDs.get(0));

		if (pos == size())
			return SID.greaterThan(posIDs.get(pos - 1));

		SID posId = posIDs.get(pos);
		return posId.between( nextId(posId) );
	}

	final private SID nextId(SID other) {
		PosID<V> fromKey = new PosID<V>(other, null);
		for (Map.Entry<SequenceVersioned.PosID<V>, Set<TripleTimestamp>> i : elems.tailMap(fromKey).entrySet())
			return i.getKey().getId();

		assert false;
		
		return null;
	}

	/**
	 * Insert element e in the supporting sorted set, using the given unique
	 * identifier.
	 * 
	 * @param e
	 */
	private void insert(PosID<V> e) {
		TripleTimestamp ts = nextTimestamp();
		e.setTimestamp(ts) ;
		
		Set<TripleTimestamp> adds = elems.get(e);
		if (adds == null) {
			adds = new HashSet<TripleTimestamp>();
			elems.put(e, adds);
		}
		adds.add(ts);
		registerLocalOperation(new SequenceInsert<PosID<V>, SequenceVersioned<V>>(ts, e));
	}

	/**
	 * Remove element e from the supporting sorted set.
	 * 
	 * @param e
	 */
	public void remove(PosID<V> e) {
		Set<TripleTimestamp> ids = elems.remove(e);
		if (ids != null) {
			TripleTimestamp ts = nextTimestamp();
			registerLocalOperation(new SequenceRemove<PosID<V>, SequenceVersioned<V>>(ts, e, ids));
		}
		elems.put( e.deletedPosID(), new HashSet<TripleTimestamp>());
	}
}
