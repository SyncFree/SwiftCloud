package swift.crdt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.interfaces.CRDTOperation;
import swift.crdt.interfaces.ICRDTInteger;
import swift.crdt.operations.IntegerAdd;
import swift.crdt.operations.IntegerSub;
import swift.exceptions.NotSupportedOperationException;

public class CRDTInteger extends BaseCRDT<CRDTInteger, ICRDTInteger, Timestamp> {
	// set of adds per site
	private Map<String, Integer> adds;
	// set of removes per site
	private Map<String, Integer> rems;
	// current value
	private int val;

	public CRDTInteger(int initial, CausalityClock c) {
		super();
		this.val = initial;
		this.adds = new HashMap<String, Integer>();
		this.rems = new HashMap<String, Integer>();
	}

	public int value() {
		return this.val;
	}

	public void add(int n) {
		Timestamp ts = getTxnHandle().nextTimestamp();
		CRDTOperation<ICRDTInteger, Timestamp> op = new IntegerAdd(getUID(),
				ts, getClock(), n);
		getTxnHandle().registerOperation(op);
	}

	private int addU(int n, Timestamp ts) {
		if (n < 0) {
			return subU(-n, ts);
		}

		String siteId = ts.getIdentifier();
		int v;
		if (this.adds.containsKey(siteId)) {
			v = this.adds.get(siteId) + n;
		} else {
			v = n;
		}

		this.adds.put(siteId, v);
		this.val += n;
		return this.val;
	}

	private int subU(int n, Timestamp ts) {
		if (n < 0) {
			return addU(-n, ts);
		}
		String siteId = ts.getIdentifier();
		int v;
		if (this.rems.containsKey(siteId)) {
			v = this.rems.get(siteId) + n;
		} else {
			v = n;
		}

		this.rems.put(siteId, v);
		this.val -= n;
		return this.val;
	}

	@Override
	protected void mergePayload(CRDTInteger that) {
		// TODO Iterate over the smaller set!
		for (Entry<String, Integer> e : that.adds.entrySet()) {
			if (!this.adds.containsKey(e.getKey())) {
				int v = e.getValue();
				this.val += v;
				this.adds.put(e.getKey(), v);
			} else {
				int v = this.adds.get(e.getKey());
				if (v < e.getValue()) {
					this.val = this.val - v + e.getValue();
					this.adds.put(e.getKey(), e.getValue());
				}
			}
		}

		for (Entry<String, Integer> e : that.rems.entrySet()) {
			if (!this.rems.containsKey(e.getKey())) {
				int v = e.getValue();
				this.val -= v;
				this.rems.put(e.getKey(), v);
			} else {
				int v = this.rems.get(e.getKey());
				if (v < e.getValue()) {
					this.val = this.val + v - e.getValue();
					this.rems.put(e.getKey(), e.getValue());
				}
			}
		}
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof CRDTInteger)) {
			return false;
		}
		CRDTInteger that = (CRDTInteger) other;
		return that.val == this.val && that.adds.equals(this.adds)
				&& that.rems.equals(this.rems);
	}

	@Override
	public void execute(CRDTOperation<ICRDTInteger, Timestamp> op) {
		if (op instanceof IntegerAdd) {
			IntegerAdd<Timestamp> addop = (IntegerAdd<Timestamp>) op;
			this.addU(addop.getVal(), addop.getTimestamp());
		} else if (op instanceof IntegerSub) {
			IntegerSub<Timestamp> subop = (IntegerSub<Timestamp>) op;
			this.subU(subop.getVal(), subop.getTimestamp());
		} else {
			throw new NotSupportedOperationException();
		}
	}

	@Override
	public void prune(CausalityClock c) {
		// TODO Auto-generated method stub

	}

	@Override
	public void rollback(Timestamp ts) {
		// TODO Auto-generated method stub

	}

	// TODO Reimplement the hashCode() method!!!

}
