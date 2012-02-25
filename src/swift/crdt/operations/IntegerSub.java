package swift.crdt.operations;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.ICRDTInteger;

public class IntegerSub<T extends Timestamp> extends
		BaseOperation<ICRDTInteger, T> {
	private int val;

	public IntegerSub(CRDTIdentifier target, T ts, CausalityClock c, int val) {
		super(target, ts, c);
		this.val = val;
	}

	public int getVal() {
		return this.val;
	}

}
