package swift.crdt.operations;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDTOperation;

public abstract class BaseOperation<I, T extends Timestamp> implements
		CRDTOperation<I, T> {
	private CRDTIdentifier target;
	private T ts;
	private CausalityClock c;

	protected BaseOperation(CRDTIdentifier target, T ts, CausalityClock c) {
		this.target = target;
		this.ts = ts;
		this.c = c;
	}

	@Override
	public CRDTIdentifier getTargetUID() {
		return this.target;
	}

	@Override
	public T getTimestamp() {
		return this.ts;
	}

	@Override
	public void setTimestamp(T ts) {
		this.ts = ts;
	}

	@Override
	public CausalityClock getDependency() {
		return this.c;
	}

}
