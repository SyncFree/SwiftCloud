package swift.crdt.operations;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.CRDTInteger;

public class IntegerAdd extends BaseOperation<CRDTInteger> {

	private int val;

	public IntegerAdd(CRDTIdentifier target, Timestamp ts, CausalityClock c,
			int val) {
		super(target, ts, c);
		this.val = val;
	}

	public int getVal() {
		return this.val;
	}

}
