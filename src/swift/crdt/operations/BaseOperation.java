package swift.crdt.operations;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CRDTOperation;

public abstract class BaseOperation<V extends CRDT<V>> implements
		CRDTOperation<V> {
	private CRDTIdentifier target;
	private Timestamp ts;
	private CausalityClock c;

	protected BaseOperation(CRDTIdentifier target, Timestamp ts,
			CausalityClock c) {
		this.target = target;
		this.ts = ts;
		this.c = c;
	}

	@Override
	public CRDTIdentifier getTargetUID() {
		return this.target;
	}

	@Override
	public Timestamp getTimestamp() {
		return this.ts;
	}

	@Override
	public void setTimestamp(Timestamp ts) {
		this.ts = ts;
	}

	@Override
	public CausalityClock getDependency() {
		return this.c;
	}

}
