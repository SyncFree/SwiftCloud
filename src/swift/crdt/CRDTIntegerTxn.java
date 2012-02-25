package swift.crdt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.ICRDTInteger;
import swift.crdt.operations.IntegerAdd;
import swift.crdt.operations.IntegerSub;
import swift.exceptions.NotSupportedOperationException;
import swift.utils.Pair;

public class CRDTIntegerTxn extends
		BaseCRDT<CRDTIntegerTxn, ICRDTInteger, TripleTimestamp> {
	private Map<String, Set<Pair<Integer, TripleTimestamp>>> adds;
	private Map<String, Set<Pair<Integer, TripleTimestamp>>> rems;
	private int val;

	public CRDTIntegerTxn(int initial) {
		this.val = initial;
		this.adds = new HashMap<String, Set<Pair<Integer, TripleTimestamp>>>();
		this.rems = new HashMap<String, Set<Pair<Integer, TripleTimestamp>>>();
	}

	public int value() {
		return this.val;
	}

	public int value(CausalityClock clk) {
		// FIXME Check!
		int retValue = 0;
		for (Entry<String, Set<Pair<Integer, TripleTimestamp>>> entry : adds
				.entrySet()) {
			for (Pair<Integer, TripleTimestamp> set : entry.getValue()) {
				if (clk.includes(set.getSecond())) {
					retValue += set.getFirst();
				}
			}
		}
		for (Entry<String, Set<Pair<Integer, TripleTimestamp>>> entry : rems
				.entrySet()) {
			for (Pair<Integer, TripleTimestamp> set : entry.getValue()) {
				if (clk.includes(set.getSecond())) {
					retValue -= set.getFirst();
				}
			}
		}
		return retValue;
	}

	public int addU(int n, TripleTimestamp ts) {
		if (n < 0) {
			return subU(-n, ts);
		}
		String siteId = ts.getIdentifier();
		Set<Pair<Integer, TripleTimestamp>> v = adds.get(siteId);
		if (v == null) {
			v = new HashSet<Pair<Integer, TripleTimestamp>>();
			adds.put(siteId, v);
		}

		// FIXME - Timestamp is not immutable, so it cannot be added into a
		// HashSet safely.
		v.add(new Pair<Integer, TripleTimestamp>(n, ts));
		val += n;
		return val;
	}

	public int subU(int n, TripleTimestamp ts) {
		if (n < 0) {
			return addU(-n, ts);
		}
		String siteId = ts.getIdentifier();
		Set<Pair<Integer, TripleTimestamp>> v = rems.get(siteId);
		if (v == null) {
			v = new HashSet<Pair<Integer, TripleTimestamp>>();
			rems.put(siteId, v);
		}
		v.add(new Pair<Integer, TripleTimestamp>(n, ts));
		val -= n;
		return val;
	}

	@Override
	public void mergePayload(CRDTIntegerTxn other) {
		for (Entry<String, Set<Pair<Integer, TripleTimestamp>>> e : other.adds
				.entrySet()) {
			Set<Pair<Integer, TripleTimestamp>> v = this.adds.get(e.getKey());
			if (v == null) {
				v = e.getValue();
				adds.put(
						e.getKey(),
						new HashSet<Pair<Integer, TripleTimestamp>>(e
								.getValue()));
			} else {
				v.addAll(e.getValue());
			}

		}

		for (Entry<String, Set<Pair<Integer, TripleTimestamp>>> e : other.rems
				.entrySet()) {
			Set<Pair<Integer, TripleTimestamp>> v = rems.get(e.getKey());
			if (v == null) {
				v = e.getValue();
				rems.put(
						e.getKey(),
						new HashSet<Pair<Integer, TripleTimestamp>>(e
								.getValue()));
			} else {
				v.addAll(e.getValue());
			}

		}

		val = 0;
		for (Set<Pair<Integer, TripleTimestamp>> v : adds.values()) {
			for (Pair<Integer, TripleTimestamp> vi : v) {
				val += vi.getFirst();
			}
		}

		for (Set<Pair<Integer, TripleTimestamp>> v : rems.values()) {
			for (Pair<Integer, TripleTimestamp> vi : v) {
				val -= vi.getFirst();
			}
		}

	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof CRDTIntegerTxn)) {
			return false;
		}
		CRDTIntegerTxn that = (CRDTIntegerTxn) other;
		return that.val == this.val && that.adds.equals(this.adds)
				&& that.rems.equals(this.rems);
	}

	@Override
	public void rollback(Timestamp rollbackEvent) {
		Iterator<Entry<String, Set<Pair<Integer, TripleTimestamp>>>> it = adds
				.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Set<Pair<Integer, TripleTimestamp>>> addForSite = it
					.next();
			Iterator<Pair<Integer, TripleTimestamp>> addTSit = addForSite
					.getValue().iterator();
			while (addTSit.hasNext()) {
				Pair<Integer, TripleTimestamp> ts = addTSit.next();
				if ((ts.getSecond()).equals(rollbackEvent)) {
					addTSit.remove();
					val -= ts.getFirst();
				}
			}
			if (addForSite.getValue().isEmpty()) {
				it.remove();
			}
		}

		it = rems.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Set<Pair<Integer, TripleTimestamp>>> remsForSite = it
					.next();
			Iterator<Pair<Integer, TripleTimestamp>> remTSit = remsForSite
					.getValue().iterator();
			while (remTSit.hasNext()) {
				Pair<Integer, TripleTimestamp> ts = remTSit.next();
				if (((TripleTimestamp) ts.getSecond()).equals(rollbackEvent)) {
					remTSit.remove();
					val += ts.getFirst();
				}
			}
			if (remsForSite.getValue().isEmpty()) {
				it.remove();
			}
		}

	}

	@Override
	public void execute(CRDTOperation<ICRDTInteger, TripleTimestamp> op) {
		if (op instanceof IntegerAdd) {
			IntegerAdd<TripleTimestamp> addop = (IntegerAdd<TripleTimestamp>) op;
			this.addU(addop.getVal(), addop.getTimestamp());
		} else if (op instanceof IntegerSub) {
			IntegerSub<TripleTimestamp> subop = (IntegerSub<TripleTimestamp>) op;
			this.subU(subop.getVal(), subop.getTimestamp());
		} else {
			throw new NotSupportedOperationException();
		}

	}

	@Override
	public void prune(CausalityClock c) {
		// TODO Auto-generated method stub

	}
}
