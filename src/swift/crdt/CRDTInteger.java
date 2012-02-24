package swift.crdt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.interfaces.CRDTOperation;

public class CRDTInteger extends BaseCRDT<CRDTInteger> {
	private static final long serialVersionUID = 1L;
	private Map<String, Integer> adds;
	private Map<String, Integer> rems;
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

	private int add(int n, String siteId) {
		if (n < 0) {
			return sub(-n, siteId);
		}

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

	private int sub(int n, String siteId) {
		if (n < 0) {
			return add(-n, siteId);
		}
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
	public void execute(CRDTOperation op) {
		// TODO Auto-generated method stub

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
